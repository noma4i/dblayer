//! Executors of the effects of Lifecycle, Store, Network and Calls (`design.md` F3). Each one runs the IO its owner asked for
//! and returns the result as an input. None of them decides a transition.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::task::AbortHandle;

use super::{LifeReply, Ports, Wiring};
use crate::db::Db;
use crate::identity::Generation;
use crate::owners::calls::{CallError, Calls, CallsEffect, CallsInput};
use crate::owners::contract::{Drained, NetworkEvent, Online, PublishedState};
use crate::owners::lifecycle::{Lifecycle, LifecycleEffect, LifecycleInput, Peer};
use crate::owners::network::{Network, NetworkEffect, NetworkInput};
use crate::owners::read::deliveries::DeliveryInput;
use crate::owners::read::feed::FeedInput;
use crate::owners::read::windows::WindowsInput;
use crate::owners::session::SessionInput;
use crate::owners::store::{Done, StoreEffect, StoreInput, StoreOp, StoreWriter};
use crate::owners::write::outbox::OutboxInput;
use crate::owners::write::transfer::TransferInput;
use crate::runtime::{Back, EffectRunner, EpochStamp, OutcomeId};
use crate::transport::{GraphQLRequest, ResponseOutcome, TransportError};

const ALL_PEERS: [Peer; 10] = crate::owners::lifecycle::ALL_OWNERS;

pub(super) struct LifecycleRunner {
    pub wiring: Arc<Wiring>,
    pub leases: Arc<crate::ffi::legacy::Leases>,
    pub absent: Vec<Peer>,
    pub db: Db,
    /// The published value and the answers of one step wait for `step_done`: a caller that sees a new epoch finds every owner already in it.
    pub held_published: Option<crate::owners::contract::Published>,
    pub held_replies: Vec<(u64, Result<(), crate::owners::lifecycle::LifecycleError>)>,
}

impl EffectRunner<Lifecycle> for LifecycleRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: LifecycleEffect, _back: &Back<Lifecycle>) {
        let wiring = &self.wiring;
        match effect {
            LifecycleEffect::Publish { publication_id, published } => {
                if matches!(published.state, PublishedState::Ending | PublishedState::EndingFailed | PublishedState::Closing | PublishedState::StorageFailed) {
                    self.held_published = Some(published.clone());
                }
                wiring.to_store(StoreInput::Published { publication_id, published });
            }
            LifecycleEffect::PublishCell(published) => {
                self.held_published = Some(published);
            }
            LifecycleEffect::RestoreReady { epoch } => wiring.to_session(SessionInput::RestoreReady { epoch }),
            LifecycleEffect::EpochStarted { epoch, identity, generation } => {
                wiring.handle.start_epoch(epoch);
                let started = crate::owners::contract::EpochStarted { epoch, identity: identity.map(|identity| identity.into()), generation };
                wiring.to_windows(WindowsInput::EpochStarted(started.clone()));
                wiring.to_deliveries(DeliveryInput::EpochStarted(started.clone()));
                wiring.to_feed(FeedInput::EpochStarted(started.clone()));
                wiring.to_outbox(OutboxInput::EpochStarted(started.clone()));
                wiring.to_transfer(TransferInput::EpochStarted(started));
                for owner in [Peer::Feed, Peer::Outbox, Peer::Transfer, Peer::Calls, Peer::Session] {
                    wiring.to_network(NetworkInput::SnapshotRequest { owner, epoch });
                }
            }
            LifecycleEffect::ReadIntents { epoch } => wiring.to_store(StoreInput::ReadIntents { epoch }),
            LifecycleEffect::EpochEnded { epoch, cause } => {
                self.leases.epoch_ended(cause);
                wiring.handle.end_epoch(epoch, cause);
                wiring.outcomes.end_epoch(epoch, cause);
            }
            LifecycleEffect::ModelsReset { epoch, generation, models } => {
                let models: Vec<String> = models.into_iter().collect();
                self.leases.models_reset(generation, &models);
                let reset = crate::owners::contract::ModelsReset { epoch, generation, models };
                wiring.to_windows(WindowsInput::ModelsReset(reset.clone()));
                wiring.to_deliveries(DeliveryInput::ModelsReset(reset.clone()));
                wiring.to_feed(FeedInput::ModelsReset(reset.clone()));
                wiring.to_outbox(OutboxInput::ModelsReset(reset));
            }
            LifecycleEffect::Closing => {
                wiring.release_held();
                wiring.handle.close();
                for peer in &self.absent {
                    wiring.to_lifecycle(LifecycleInput::Drained(Drained { owner: peer.name() }));
                }
            }
            LifecycleEffect::StopObserving => {
                let done = super::StopDone::new(wiring.clone());
                match self.db.shared.os_observers.get() {
                    Some(port) => crate::db::lock::effect(|| port.stop(done)),
                    None => done.finish(),
                }
            }
            LifecycleEffect::ClearAccount { epoch_old, cause, generation_new, erase_id } => wiring.to_store(StoreInput::ClearAccount { epoch_old, cause, generation_new, erase_id }),
            LifecycleEffect::ClearModels { epoch, generation_new, models, erase_id } => wiring.to_store(StoreInput::ClearModels { epoch, generation_new, models, erase_id }),
            LifecycleEffect::StoreClose => wiring.to_store(StoreInput::Close),
            LifecycleEffect::Reply { id, result } => self.held_replies.push((id, result)),
        }
    }

    fn step_done(&mut self) {
        if let Some(published) = self.held_published.take() {
            self.leases.published(&published);
            self.wiring.publish(published.clone());
            self.wiring.to_session(SessionInput::Published(published));
        }
        for (id, result) in self.held_replies.drain(..) {
            if !self.wiring.open_answered(id, result) {
                self.wiring.outcomes.settle(OutcomeId::from_raw(id), LifeReply::Life(result));
            }
        }
    }
}

fn revision_of(revision: i64) -> Result<u64, String> { u64::try_from(revision).map_err(|_| format!("the stored revision {revision} is not a revision")) }

pub(super) struct StoreRunner {
    pub wiring: Arc<Wiring>,
    pub db: Db,
    pub credentials: Option<Arc<dyn crate::transport::CredentialSlot>>,
    pub client: Option<Arc<crate::transport::EncryptedClient>>,
    pub executor: tokio::runtime::Handle,
}

impl StoreRunner {
    fn erased(&self, back: &Back<StoreWriter>, io_id: u64, result: Result<i64, crate::store::StoreError>) {
        let result = result.map_err(|error| error.to_string()).and_then(|revision| revision_of(revision).map(|revision| Done { revision }));
        back.reply(EpochStamp::Control, StoreInput::Erased { io_id, result });
    }

    fn revision(&self) -> Result<u64, String> { self.db.shared.store.revision_seq().map_err(|error| error.to_string()).and_then(revision_of) }
}

impl EffectRunner<StoreWriter> for StoreRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: StoreEffect, back: &Back<StoreWriter>) {
        let wiring = &self.wiring;
        let store = &self.db.shared.store;
        match effect {
            StoreEffect::Prepare { .. } => {}
            StoreEffect::OpenLifecycle(message) => {
                wiring.open_session();
                wiring.to_lifecycle(LifecycleInput::Open {
                    saved_epoch: message.saved_epoch,
                    saved_generation: message.saved_generation,
                    saved_identity: message.saved_identity,
                    reply: wiring.open_reply(),
                });
            }
            StoreEffect::OpenFailed(error) => wiring.open_failed(error, false),
            StoreEffect::OpenCorrupt(error) => wiring.open_failed(error, true),
            StoreEffect::Write { batch } => {
                let result = write_batch(&self.db, &batch).and_then(|()| self.revision()).map(|revision| Done { revision });
                back.reply(EpochStamp::Control, StoreInput::Written { author: batch.author, batch: batch.id, result });
            }
            StoreEffect::EraseAccount { io_id, generation_new, erase_encrypted } => {
                self.erased(back, io_id, store.erase_account(Generation::from_value(generation_new), erase_encrypted));
            }
            StoreEffect::EraseModels { io_id, generation_new, models } => {
                let models: Vec<String> = models.into_iter().collect();
                self.erased(back, io_id, store.erase_models(Generation::from_value(generation_new), &models));
            }
            StoreEffect::ClearSecret { io_id } => {
                let (client, credentials, wiring, back) = (self.client.clone(), self.credentials.clone(), wiring.clone(), back.clone());
                drop(crate::task::spawn_blocking_on(crate::task::Carry::Detached, &self.executor, move || {
                    let cleared = match (&client, &credentials) {
                        (Some(client), _) => client.erase_credential(),
                        (None, Some(credentials)) => credentials.clear(),
                        (None, None) => Ok(()),
                    };
                    if let Err(error) = &cleared {
                        wiring.fault(format!("clear secret: {error}"));
                    }
                    back.reply(EpochStamp::Control, StoreInput::Erased { io_id, result: cleared.map(|()| Done { revision: 0 }).map_err(|error| error.to_string()) });
                }));
            }
            StoreEffect::SavePublished { io_id, published, .. } => {
                let saved = store.save_published(published.epoch, published.generation).map_err(|error| error.to_string()).and_then(|()| self.revision()).map(|revision| Done { revision });
                back.reply(EpochStamp::Control, StoreInput::Erased { io_id, result: saved });
            }
            StoreEffect::Reply { author, batch, epoch, reply } => wiring.route_store_reply(author, batch, epoch, reply),
            StoreEffect::BroadcastCommitted { revision, models } => wiring.tell_committed(crate::owners::contract::CommittedBroadcast {
                revision,
                tables: Vec::new(),
                models: models.map_or(crate::owners::contract::Models::All, |models| crate::owners::contract::Models::Set(models.into_iter().collect())),
            }),
            StoreEffect::Cleared { erase_id, .. } => wiring.to_lifecycle(LifecycleInput::Cleared { erase_id }),
            StoreEffect::ModelsCleared { generation_new, erase_id } => wiring.to_lifecycle(LifecycleInput::ModelsCleared { generation_new, erase_id }),
            StoreEffect::ClearFailed { erase_id, error } => wiring.to_lifecycle(LifecycleInput::ClearFailed { erase_id, error }),
            StoreEffect::Saved { publication_id } => wiring.to_lifecycle(LifecycleInput::Saved { publication_id }),
            StoreEffect::SaveFailed { publication_id, error } => wiring.to_lifecycle(LifecycleInput::SaveFailed { publication_id, error }),
            StoreEffect::ReadIntents { epoch } => {
                let restore = self.db.read_restore();
                let fail = |wiring: &Wiring, error: String| {
                    wiring.restore_failed_for(Peer::Outbox, error.clone());
                    wiring.to_outbox(OutboxInput::RestoreFailed { error: error.clone() });
                    if wiring.has_transfer() {
                        wiring.restore_failed_for(Peer::Transfer, error.clone());
                        wiring.to_transfer(TransferInput::RestoreFailed { error });
                    }
                };
                match restore {
                    Err(error) => fail(wiring, error.to_string()),
                    Ok(restore) => {
                        match restore.ops {
                            Ok(stored) => wiring.to_outbox(OutboxInput::Restored { ops: stored, pending_lost: false }),
                            Err(error) => {
                                wiring.restore_failed_for(Peer::Outbox, error.to_string());
                                wiring.to_outbox(OutboxInput::RestoreFailed { error: error.to_string() });
                            }
                        }
                        if wiring.has_transfer() {
                            match restore.transfers {
                                Ok(stored) => wiring.to_transfer(TransferInput::Restored(stored)),
                                Err(error) => {
                                    wiring.restore_failed_for(Peer::Transfer, error.to_string());
                                    wiring.to_transfer(TransferInput::RestoreFailed { error: error.to_string() });
                                }
                            }
                        }
                    }
                }
                match &self.client {
                    Some(client) => match super::session_runner::SessionRunner::rows(client) {
                        Ok(stored) => wiring.to_session(SessionInput::Restored { epoch, pending: stored.pending, answers: stored.answers, pending_lost: stored.dropped_pending }),
                        Err(error) => wiring.restore_failed_for(Peer::Session, error.to_string()),
                    },
                    None => wiring.to_lifecycle(LifecycleInput::RestoreClassified { owner: Peer::Session, epoch }),
                }
            }
            StoreEffect::CloseReads => self.db.shared.store.reads().close(),
            StoreEffect::CloseConnections => {
                if let Err(error) = self.db.close() {
                    wiring.fault(format!("close: {error}"));
                }
            }
            StoreEffect::StoreClosed => wiring.to_lifecycle(LifecycleInput::StoreClosed),
        }
    }
}

pub(super) struct NetworkRunner {
    pub wiring: Arc<Wiring>,
    pub db: Db,
    /// The answer of `set_online` waits for `step_done`: a caller that heard it finds `Online` already queued for every owner.
    pub held_replies: Vec<(u64, bool)>,
}

impl EffectRunner<Network> for NetworkRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: NetworkEffect, _back: &Back<Network>) {
        let wiring = &self.wiring;
        match effect {
            NetworkEffect::Broadcast(online) => {
                self.db.connectivity().change(online);
                wiring.to_windows(WindowsInput::Online(Online(online)));
                wiring.to_feed(FeedInput::Online(Online(online)));
                wiring.to_outbox(OutboxInput::Online(Online(online)));
                wiring.to_transfer(TransferInput::Online(Online(online)));
                wiring.to_session(SessionInput::Online(Online(online)));
                wiring.to_calls(CallsInput::Online(Online(online)));
                wiring.tell_network(&ALL_PEERS, NetworkEvent::Online { online, epoch: None });
            }
            NetworkEffect::Snapshot { owner, epoch, online, saving } => {
                if wiring.epoch_passed(epoch) {
                    return;
                }
                let stamp = EpochStamp::Epoch(epoch);
                match owner {
                    Peer::Windows => wiring.to_windows_in(epoch, WindowsInput::Online(Online(online))),
                    Peer::Feed => wiring.to_feed(FeedInput::Online(Online(online))),
                    Peer::Calls => wiring.to_calls_at(stamp, CallsInput::Online(Online(online))),
                    Peer::Outbox => wiring.to_outbox_at(stamp, OutboxInput::Online(Online(online))),
                    Peer::Transfer => wiring.to_transfer_at(stamp, TransferInput::Online(Online(online))),
                    Peer::Session => wiring.to_session_at(stamp, SessionInput::Snapshot { epoch, online }),
                    _ => {}
                }
                wiring.tell_network(&[owner], NetworkEvent::Online { online, epoch: Some(epoch) });
                if let Some(saving) = saving {
                    wiring.tell_network(&[owner], NetworkEvent::PowerSaving { saving, epoch: Some(epoch) });
                }
            }
            NetworkEffect::Foreground(seq) => {
                wiring.to_windows(WindowsInput::Foreground(seq));
                wiring.tell_network(&[Peer::Rounds, Peer::Session, Peer::Windows], NetworkEvent::Foreground(seq));
            }
            NetworkEffect::PowerSaving(saving) => wiring.tell_network(&[Peer::Transfer], NetworkEvent::PowerSaving { saving, epoch: None }),
            NetworkEffect::Drained(drained) => wiring.to_lifecycle(LifecycleInput::Drained(drained)),
            NetworkEffect::Reply { id, closed } => self.held_replies.push((id, closed)),
        }
    }

    fn step_done(&mut self) {
        for (id, closed) in self.held_replies.drain(..) {
            self.wiring.outcomes.settle(OutcomeId::from_raw(id), LifeReply::Online { closed });
        }
    }
}

pub(super) struct CallsRunner {
    wiring: Arc<Wiring>,
    transport: Option<Arc<dyn crate::transport::Transport>>,
    executor: tokio::runtime::Handle,
    operations: super::OperationLookup,
    db: Db,
    running: HashMap<u64, AbortHandle>,
}

impl CallsRunner {
    pub fn new(wiring: Arc<Wiring>, ports: &Ports) -> Self {
        Self { wiring, transport: ports.transport.clone(), executor: ports.executor.clone(), operations: ports.operations.clone(), db: ports.db.clone(), running: HashMap::new() }
    }

    fn input_of(call_id: u64, root: &str, result: Result<Result<crate::transport::Response, TransportError>, crate::ffi::legacy::Ended>) -> CallsInput {
        match result {
            Ok(Ok(response)) => {
                if response.shape().outcome() != ResponseOutcome::Commit {
                    let messages: Vec<String> = response.errors.iter().map(|error| error.message.clone()).collect();
                    return CallsInput::Response { call_id, body: String::new(), errors: if messages.is_empty() { vec!["empty response".to_string()] } else { messages } };
                }
                let data = response.data.unwrap_or(Value::Null);
                CallsInput::Response { call_id, body: data.get(root).cloned().unwrap_or(Value::Null).to_string(), errors: Vec::new() }
            }
            Ok(Err(TransportError::Graphql { messages, .. })) => CallsInput::TransportError { call_id, error: CallError::Graphql(messages) },
            Ok(Err(TransportError::Http { .. })) => CallsInput::TransportError { call_id, error: CallError::Http },
            Ok(Err(TransportError::Interrupted(_))) | Err(_) => CallsInput::TransportError { call_id, error: CallError::Interrupted },
            Ok(Err(_)) => CallsInput::TransportError { call_id, error: CallError::Network },
        }
    }
}

impl EffectRunner<Calls> for CallsRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: CallsEffect, back: &Back<Calls>) {
        match effect {
            CallsEffect::Request { call_id, request_id, operation, variables } => {
                let (Some(transport), Some((document, root))) = (self.transport.clone(), (self.operations)(&operation)) else {
                    back.reply(EpochStamp::Control, CallsInput::TransportError { call_id, error: CallError::Invalid });
                    return;
                };
                let back = back.clone();
                let lease = self.db.shared.store.lifecycle.clone();
                let task = crate::task::spawn_on(crate::task::Carry::Detached, &self.executor, async move {
                    let admitted = match lease.admit(crate::ffi::legacy::Origin::Explicit, "run") {
                        Ok(admitted) => admitted,
                        Err(_) => return back.reply(EpochStamp::Control, CallsInput::TransportError { call_id, error: CallError::Interrupted }),
                    };
                    let variables: Value = serde_json::from_str(&variables).unwrap_or(Value::Null);
                    let request = GraphQLRequest::new(document.clone(), operation, variables);
                    crate::db::lock::assert_no_library_lock("Transport::request");
                    let result = admitted.run(async {
                        if document.trim_start().starts_with("mutation") { transport.mutate(&admitted, request).await } else { transport.query(&admitted, request).await }
                    }).await;
                    back.reply(EpochStamp::Control, Self::input_of(call_id, &root, result));
                });
                self.running.insert(request_id, task.abort_handle());
            }
            CallsEffect::Cancel { request_id } => {
                if let Some(task) = self.running.remove(&request_id) {
                    crate::db::lock::effect(|| task.abort());
                }
            }
            CallsEffect::Reply { id, answer } => {
                self.wiring.outcomes.settle(OutcomeId::from_raw(id), LifeReply::Call(answer));
            }
            CallsEffect::Drained(drained) => self.wiring.to_lifecycle(LifecycleInput::Drained(drained)),
        }
    }
}

/// One transaction per kind of ops: the tables `encrypted_*` through the store, the queue through the commit path of `Db`.
pub(super) fn write_batch(db: &Db, batch: &crate::owners::store::StoreBatch) -> Result<(), String> {
    let (encrypted, queue): (Vec<&StoreOp>, Vec<&StoreOp>) = batch.ops.iter().partition(|op| matches!(op, StoreOp::Pending { .. } | StoreOp::RemovePending | StoreOp::EraseSession | StoreOp::Answer { .. } | StoreOp::RemoveAnswer { .. } | StoreOp::Session { .. }));
    let (removals, encrypted): (Vec<&StoreOp>, Vec<&StoreOp>) = encrypted.into_iter().partition(|op| matches!(op, StoreOp::RemoveAnswer { .. }));
    let row_ops = if batch.ops.iter().any(|op| matches!(op, StoreOp::Window { .. })) { Vec::new() } else { row_ops(&batch.rows)? };
    if !encrypted.is_empty() {
        db.shared.store.write_ops(encrypted.into_iter().filter_map(row_op).collect()).map(drop).map_err(|error| error.to_string())?;
    }
    if !queue.is_empty() || !row_ops.is_empty() {
        let ops: Vec<StoreOp> = row_ops.into_iter().chain(queue.into_iter().cloned()).collect();
        db.write_queue_ops(&ops).map_err(|error| error.to_string())?;
    }
    if removals.is_empty() {
        return Ok(());
    }
    db.shared.store.write_ops(removals.into_iter().filter_map(row_op).collect()).map(drop).map_err(|error| error.to_string())
}

fn row_ops(rows: &[(String, String)]) -> Result<Vec<StoreOp>, String> {
    let mut ops: Vec<StoreOp> = Vec::new();
    for (model, text) in rows {
        let value: Value = serde_json::from_str(text).map_err(|error| format!("a row of {model} is not JSON: {error}"))?;
        let Some(row_id) = value.get("id").and_then(Value::as_str) else { return Err(format!("a row of {model} has no id")) };
        let row = crate::owners::store::RowValue { row_id: row_id.to_string(), values: text.clone() };
        match ops.last_mut() {
            Some(StoreOp::Rows { model: last, rows }) if last == model => rows.push(row),
            _ => ops.push(StoreOp::Rows { model: model.clone(), rows: vec![row] }),
        }
    }
    Ok(ops)
}

/// The batch op as the tables hold it: ids as text, the purpose as the enum of the transport.
fn row_op(op: &StoreOp) -> Option<crate::store::RowOp> {
    Some(match op {
        StoreOp::Pending { pending, credential_digest, wire_id, response_key } => crate::store::RowOp::PutPending(crate::transport::encrypted::state::Pending {
            credential_digest: credential_digest.clone(),
            request_id: wire_id.clone(),
            response_key: response_key.clone(),
            sequence: i64::try_from(pending.sequence).unwrap_or(i64::MAX),
            path: pending.path.clone(),
            body: pending.body.clone(),
            owner: pending.owner.clone(),
            purpose: if pending.purpose == 0 { crate::transport::NetworkPurpose::Standard } else { crate::transport::NetworkPurpose::Transfer },
            req_id: pending.request_id,
        }),
        StoreOp::RemovePending => crate::store::RowOp::RemovePending,
        StoreOp::EraseSession => crate::store::RowOp::RemoveSession,
        StoreOp::Answer { request_id, operation, result } => crate::store::RowOp::PutAnswer { request_id: *request_id, operation: *operation, answer: result.clone() },
        StoreOp::RemoveAnswer { request_id } => crate::store::RowOp::RemoveAnswer { request_id: *request_id },
        StoreOp::Session { digest, sequence } => crate::store::RowOp::PutSession { digest: digest.clone(), sequence: sequence.map(|sequence| i64::try_from(sequence).unwrap_or(i64::MAX)) },
        _ => return None,
    })
}
