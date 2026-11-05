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
use crate::owners::contract::{Drained, Online};
use crate::owners::lifecycle::{Lifecycle, LifecycleEffect, LifecycleInput, Peer};
use crate::owners::network::{Network, NetworkEffect, NetworkInput};
use crate::owners::session::SessionInput;
use crate::owners::store::{StoreEffect, StoreInput, StoreOp, StoreWriter};
use crate::runtime::{Back, EffectRunner, EpochStamp, OutcomeId};
use crate::transport::{GraphQLRequest, ResponseOutcome, TransportError};

pub(super) struct LifecycleRunner {
    pub wiring: Arc<Wiring>,
    pub leases: Arc<crate::ffi::legacy::Leases>,
    pub absent: Vec<Peer>,
    /// The published value and the answers of one step wait for `step_done`: a caller that sees a new epoch finds every owner already in it.
    pub held_published: Option<crate::owners::contract::Published>,
    pub held_replies: Vec<(u64, Result<(), crate::owners::lifecycle::LifecycleError>)>,
}

impl EffectRunner<Lifecycle> for LifecycleRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: LifecycleEffect, _back: &Back<Lifecycle>) {
        let wiring = &self.wiring;
        match effect {
            LifecycleEffect::Publish(published) => {
                self.held_published = Some(published.clone());
                wiring.to_session(SessionInput::Published(published.clone()));
                wiring.to_store(StoreInput::Published(published));
            }
            LifecycleEffect::EpochStarted { epoch, .. } => {
                wiring.set_ending(false);
                wiring.handle.start_epoch(epoch);
                wiring.to_network(NetworkInput::EpochStarted);
            }
            LifecycleEffect::EpochEnded { epoch, cause } => {
                self.leases.epoch_ended(cause);
                wiring.set_ending(true);
                wiring.handle.end_epoch(epoch, cause);
                wiring.outcomes.end_epoch(epoch, cause);
            }
            LifecycleEffect::ModelsReset { generation, models, .. } => self.leases.models_reset(generation, &models.into_iter().collect::<Vec<_>>()),
            LifecycleEffect::Closing => {
                wiring.handle.close();
                for peer in &self.absent {
                    wiring.to_lifecycle(LifecycleInput::Drained(Drained { owner: peer.name() }));
                }
            }
            LifecycleEffect::ClearAccount { epoch_old, cause, generation_new } => wiring.to_store(StoreInput::ClearAccount { epoch_old, cause, generation_new }),
            LifecycleEffect::ClearModels { epoch, generation_new, models } => wiring.to_store(StoreInput::ClearModels { epoch, generation_new, models }),
            LifecycleEffect::StoreClose => wiring.to_store(StoreInput::Close),
            LifecycleEffect::ResetModelsSelf(models) => wiring.to_lifecycle(LifecycleInput::ResetModels { models, reply: None }),
            LifecycleEffect::Reply { id, result } => self.held_replies.push((id, result)),
        }
    }

    fn step_done(&mut self) {
        if let Some(published) = self.held_published.take() {
            self.leases.published(&published);
            self.wiring.publish(published);
        }
        for (id, result) in self.held_replies.drain(..) {
            self.wiring.outcomes.settle(OutcomeId::from_raw(id), LifeReply::Life(result));
        }
    }
}

pub(super) struct StoreRunner {
    pub wiring: Arc<Wiring>,
    pub db: Db,
    pub credentials: Option<Arc<dyn crate::transport::CredentialSlot>>,
    pub client: Option<Arc<crate::transport::EncryptedClient>>,
}

impl StoreRunner {
    fn erased(&self, back: &Back<StoreWriter>, result: Result<i64, crate::store::StoreError>) {
        back.reply(EpochStamp::Control, StoreInput::Erased { result: result.map(drop).map_err(|error| error.to_string()) });
    }
}

impl EffectRunner<StoreWriter> for StoreRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: StoreEffect, back: &Back<StoreWriter>) {
        let wiring = &self.wiring;
        let store = &self.db.shared.store;
        match effect {
            StoreEffect::Prepare { .. } => {}
            StoreEffect::OpenLifecycle(message) => wiring.to_lifecycle(LifecycleInput::Open {
                saved_epoch: message.saved_epoch,
                saved_generation: message.saved_generation,
                saved_identity: message.saved_identity,
                changed_models: message.changed_models,
            }),
            StoreEffect::OpenFailed(error) => wiring.open_failed(error),
            StoreEffect::Write { batch } => {
                let result = if batch.rows.is_empty() {
                    store.write_ops(batch.ops.iter().map(row_op).collect()).map(drop).map_err(|error| error.to_string())
                } else {
                    Err("`rows` are written by the owners of Window and Feed: their executors extend `Write`".to_string())
                };
                back.reply(EpochStamp::Control, StoreInput::Written { author: batch.author, batch: batch.id, result });
            }
            StoreEffect::EraseAccount { generation_new, erase_encrypted } => {
                if erase_encrypted {
                    if let Some(credentials) = &self.credentials {
                        if let Err(error) = credentials.clear() {
                            back.reply(EpochStamp::Control, StoreInput::Erased { result: Err(error.to_string()) });
                            return;
                        }
                    }
                }
                if erase_encrypted {
                    if let Some(client) = &self.client { client.forget_credential(); }
                }
                self.erased(back, store.erase_account(Generation::from_value(generation_new), erase_encrypted));
            }
            StoreEffect::EraseModels { generation_new, models } => {
                let models: Vec<String> = models.into_iter().collect();
                self.erased(back, store.erase_models(Generation::from_value(generation_new), &models));
            }
            StoreEffect::Reply { author, batch, reply } => wiring.route_store_reply(author, batch, reply),
            StoreEffect::BroadcastCommitted { .. } => {}
            StoreEffect::Cleared { epoch_old } => wiring.to_lifecycle(LifecycleInput::Cleared { epoch_old }),
            StoreEffect::ModelsCleared { generation_new } => wiring.to_lifecycle(LifecycleInput::ModelsCleared { generation_new }),
            StoreEffect::ClearFailed(error) => wiring.to_lifecycle(LifecycleInput::ClearFailed { error }),
            StoreEffect::SavePublished { epoch, generation } => {
                if let Err(error) = store.save_published(epoch, generation) {
                    wiring.fault(format!("save published: {error}"));
                }
            }
            StoreEffect::CloseConnections => {
                if let Err(error) = self.db.close() {
                    wiring.fault(format!("close: {error}"));
                }
                wiring.store_closed();
            }
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
                wiring.to_session(SessionInput::Online(Online(online)));
                wiring.to_calls(CallsInput::Online(Online(online)));
            }
            NetworkEffect::EpochOnline(online) => {
                wiring.to_calls(CallsInput::Online(Online(online)));
                wiring.to_session(SessionInput::Online(Online(online)));
            }
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

/// The batch op as the tables hold it: ids as text, the purpose as the enum of the transport.
fn row_op(op: &StoreOp) -> crate::store::RowOp {
    match op {
        StoreOp::PutPending { pending, credential_digest, response_key } => crate::store::RowOp::PutPending(crate::transport::encrypted::state::Pending {
            credential_digest: credential_digest.clone(),
            request_id: pending.request_id.to_string(),
            response_key: response_key.clone(),
            sequence: i64::try_from(pending.sequence).unwrap_or(i64::MAX),
            path: pending.path.clone(),
            body: pending.body.clone(),
            owner: pending.owner.clone(),
            purpose: if pending.purpose == 0 { crate::transport::NetworkPurpose::Standard } else { crate::transport::NetworkPurpose::Transfer },
        }),
        StoreOp::RemovePending => crate::store::RowOp::RemovePending,
        StoreOp::PutSession { digest, sequence } => crate::store::RowOp::PutSession { digest: digest.clone(), sequence: sequence.map(|sequence| i64::try_from(sequence).unwrap_or(i64::MAX)) },
    }
}
