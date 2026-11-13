//! Executors of the effects of Lifecycle, Store, Network and Calls. Each one runs the IO its owner asked for
//! and returns the result as an input. None of them decides a transition.

use std::sync::Arc;

use super::{LifeReply, Wiring};
use crate::store::Store;
use crate::identity::Generation;
use crate::owners::calls::CallsInput;
use crate::owners::contract::{Drained, NetworkEvent, Online, PublishedState};
use crate::owners::lifecycle::{Lifecycle, LifecycleEffect, LifecycleInput, Peer};
use crate::owners::network::{Network, NetworkEffect};
use crate::owners::read::deliveries::DeliveryInput;
use crate::owners::read::feed::FeedInput;
use crate::owners::read::windows::WindowsInput;
use crate::owners::session::{PendingCall, SessionInput};
use crate::owners::store::{Done, StoreEffect, StoreInput, StoreWriter, WriteDone};
use crate::owners::write::outbox::OutboxInput;
use crate::owners::write::rounds::RoundsInput;
use crate::owners::write::transfer::TransferInput;
use crate::runtime::{Back, EffectRunner, EpochStamp, OutcomeId};

const ALL_PEERS: [Peer; 10] = crate::owners::lifecycle::ALL_OWNERS;

pub(super) struct LifecycleRunner {
    pub wiring: Arc<Wiring>,
    pub absent: Vec<Peer>,
    pub db: std::sync::Arc<Store>,
    /// The published value and the answers of one step wait for `step_done`: a caller that sees a new epoch finds every owner already in it.
    pub held_published: Option<crate::owners::contract::Published>,
    pub held_replies: Vec<(u64, Result<(), crate::owners::lifecycle::LifecycleError>)>,
    pub held_read: Option<crate::runtime::Epoch>,
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
            LifecycleEffect::RestoreReady { epoch } => {
                wiring.to_outbox(OutboxInput::RestoreReady { epoch });
                wiring.to_transfer(TransferInput::RestoreReady { epoch });
                wiring.to_session(SessionInput::RestoreReady { epoch });
            }
            LifecycleEffect::EpochStarted { epoch, identity, generation } => {
                wiring.gate.open(epoch);
                wiring.handle.start_epoch(epoch);
                let started = crate::owners::contract::EpochStarted { epoch, identity, generation };
                wiring.to_windows(WindowsInput::EpochStarted(started.clone()));
                wiring.to_deliveries(DeliveryInput::EpochStarted(started.clone()));
                wiring.to_feed(FeedInput::EpochStarted(started.clone()));
                for owner in self.absent.iter().copied().filter(|owner| matches!(owner, Peer::Outbox | Peer::Transfer | Peer::Session)) {
                    wiring.to_lifecycle(LifecycleInput::Started { owner, epoch });
                    wiring.to_lifecycle(LifecycleInput::RestoreClassified { owner, epoch });
                }
                wiring.to_outbox(OutboxInput::EpochStarted(started.clone()));
                wiring.to_local_writes(OutboxInput::EpochStarted(started.clone()));
                wiring.to_rounds(RoundsInput::EpochStarted(started.clone()));
                wiring.to_transfer(TransferInput::EpochStarted(started));
            }
            LifecycleEffect::ReadIntents { epoch } => self.held_read = Some(epoch),
            LifecycleEffect::EpochEnded { epoch, cause } => {
                wiring.gate.end(epoch, cause);
                wiring.outcomes.end_epoch(epoch, cause);
                wiring.mutations.end_epoch(epoch, cause);
                wiring.calls_answers.end_epoch(epoch, cause);
                wiring.to_rounds(RoundsInput::EpochEnded(crate::owners::contract::EpochEnded { epoch, cause }));
                wiring.handle.end_epoch(epoch, cause);
            }
            LifecycleEffect::Closing => {
                wiring.gate.shutdown();
                wiring.handle.close();
                for peer in &self.absent {
                    wiring.to_lifecycle(LifecycleInput::Drained(Drained { owner: peer.name() }));
                }
            }
            LifecycleEffect::StopObserving => {
                let done = super::StopDone::new(wiring.clone());
                match self.db.observers().get() {
                    Some(port) => crate::lock::effect(|| port.stop(done)),
                    None => done.finish(),
                }
            }
            LifecycleEffect::ClearAccount { epoch_old, cause, generation_new, erase_id } => wiring.to_store(StoreInput::ClearAccount { epoch_old, cause, generation_new, erase_id }),
            LifecycleEffect::StoreClose => wiring.to_store(StoreInput::Close),
            LifecycleEffect::Reply { id, result } => self.held_replies.push((id, result)),
        }
    }

    fn step_done(&mut self) {
        if let Some(published) = self.held_published.take() {
            self.wiring.publish(published.clone());
            self.wiring.to_session(SessionInput::Published(published));
        }
        if let Some(epoch) = self.held_read.take() {
            self.wiring.to_store(StoreInput::ReadIntents { epoch });
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
    pub db: std::sync::Arc<Store>,
    pub secret: Option<Arc<crate::transport::encrypted::credential::Secret>>,
    pub exchange: Option<Arc<crate::transport::Exchange>>,
    pub executor: tokio::runtime::Handle,
    pub restore_error: Option<String>,
    pub session_open: Option<SessionInput>,
}

impl StoreRunner {
    fn erased(&self, back: &Back<StoreWriter>, io_id: u64, result: Result<i64, crate::store::StoreError>) {
        let result = result.map_err(|error| error.to_string()).and_then(|revision| revision_of(revision).map(|revision| Done { revision }));
        back.reply(EpochStamp::Control, StoreInput::Erased { io_id, result });
    }

    fn revision(&self) -> Result<u64, String> { self.db.revision_seq().map_err(|error| error.to_string()).and_then(revision_of) }
}

impl EffectRunner<StoreWriter> for StoreRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: StoreEffect, back: &Back<StoreWriter>) {
        let wiring = &self.wiring;
        let store = &*self.db;
        match effect {
            StoreEffect::Prepare { .. } => {}
            StoreEffect::OpenLifecycle(message) => {
                store.adopt_generation(Generation::from_value(message.saved_generation));
                if let Some(open) = self.session_open.take() { wiring.to_session(open); }
                wiring.to_lifecycle(LifecycleInput::Open {
                    saved_epoch: message.saved_epoch,
                    saved_generation: message.saved_generation,
                    saved_identity: message.saved_identity,
                    saved_unowned_pending: message.saved_unowned_pending,
                    reply: wiring.open_reply(),
                });
            }
            StoreEffect::OpenFailed(error) => wiring.open_failed(error, false),
            StoreEffect::OpenCorrupt(error) => wiring.open_failed(error, true),
            StoreEffect::Write { batch, min_pin_revision } => {
                let result = store.write_batch(&batch, self.db.files(), min_pin_revision).map(|written| {
                    for fault in written.faults { wiring.fault(fault); }
                    WriteDone { revision: written.revision, observed: written.observed, expiring: written.expiring, removed_memory: written.removed_memory, stale_parents: written.stale_parents, removed_operations: written.removed_operations, removed_rows: written.removed_rows, resets: written.resets, notice: written.notice, shown: written.shown, files_removed: written.files_removed, refused: written.refused }
                }).map_err(|error| error.reject_reason());
                back.reply(EpochStamp::Control, StoreInput::Written { author: batch.author, batch: batch.id, result });
            }
            StoreEffect::Pinned { epoch, request_id, base_revision } => wiring.to_windows_in(epoch, WindowsInput::Pinned { request_id, base_revision }),
            StoreEffect::EraseAccount { io_id, generation_new, erase_encrypted } => {
                self.erased(back, io_id, store.erase_account(Generation::from_value(generation_new), erase_encrypted));
            }
            StoreEffect::ClearSecret { io_id } => {
                let (secret, wiring, back) = (self.secret.clone(), wiring.clone(), back.clone());
                let launch = wiring.handle.launch();
                drop(crate::task::spawn_blocking_on(&self.executor, Some(launch), move || {
                    let cleared = match &secret {
                        Some(secret) => secret.erase(),
                        None => Ok(()),
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
            StoreEffect::Reply { author, batch, stamp, reply } => wiring.route_store_reply(author, batch, stamp, reply),
            StoreEffect::Observed { epoch, batch, payloads } => wiring.to_feed_at(EpochStamp::Epoch(epoch), FeedInput::Observed { batch, payloads }),
            StoreEffect::ArmExpiry { epoch, model, row_id, revision, deadline } => {
                wiring.to_local_writes_at(EpochStamp::Epoch(epoch), OutboxInput::ArmExpiry { model, row: row_id, revision, deadline });
            }
            StoreEffect::DisarmExpiry { epoch, model, row_id } => wiring.to_local_writes_at(EpochStamp::Epoch(epoch), OutboxInput::DisarmExpiry { model, row: row_id }),
            StoreEffect::ArmReset { epoch, reset } => wiring.to_local_writes_at(EpochStamp::Epoch(epoch), OutboxInput::ArmReset { reset }),
            StoreEffect::RowsRemoved { epoch, rows, revision } => for (model, row_id) in rows { wiring.to_outbox_at(EpochStamp::Epoch(epoch), OutboxInput::RowRemoved { model, row_id, revision }); },
            StoreEffect::OperationsGone { epoch, ops } => wiring.to_local_writes_at(EpochStamp::Epoch(epoch), OutboxInput::RowsGone { ops }),
            StoreEffect::BroadcastCommitted(notice) => wiring.tell_committed(notice),
            StoreEffect::Cleared { erase_id, .. } => wiring.to_lifecycle(LifecycleInput::Cleared { erase_id }),
            StoreEffect::ClearFailed { erase_id, error } => wiring.to_lifecycle(LifecycleInput::ClearFailed { erase_id, error }),
            StoreEffect::Saved { publication_id } => wiring.to_lifecycle(LifecycleInput::Saved { publication_id }),
            StoreEffect::SaveFailed { publication_id, error } => wiring.to_lifecycle(LifecycleInput::SaveFailed { publication_id, error }),
            StoreEffect::ReadIntents { epoch, direct } => {
                let restore = if direct { Ok(None) } else { self.db.read_restore().map(Some) };
                let orphan_pending = restore.as_ref().is_ok_and(|read| read.as_ref().is_some_and(|read| read.transfer_pending_orphan));
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
                    Ok(None) => {}
                    Ok(Some(restore)) => {
                        wiring.to_outbox(OutboxInput::AliasesRestored { aliases: restore.aliases, ended: restore.ended_temps });
                        if let Some(op_id) = restore.reserved {
                            wiring.to_outbox(OutboxInput::ReservedFloor { op_id });
                        }
                        match restore.ops {
                            Ok(mut stored) => {
                                if let Some(exchange) = &self.exchange {
                                    for item in &mut stored { item.op.path = exchange.graphql_path().to_string(); }
                                }
                                wiring.to_outbox(OutboxInput::OrphanTransfers { op_ids: restore.orphan_transfer_ops });
                                wiring.to_outbox(OutboxInput::Restored { ops: stored, pending_lost: false });
                                wiring.to_outbox(OutboxInput::OrphanAnswers { request_ids: restore.orphan_answers.clone() });
                            }
                            Err(error) => {
                                wiring.restore_failed_for(Peer::Outbox, error.to_string());
                                wiring.to_outbox(OutboxInput::RestoreFailed { error: error.to_string() });
                            }
                        }
                        if wiring.has_transfer() {
                            match restore.transfers {
                                Ok(stored) => { wiring.to_transfer(TransferInput::ReservedFloor { transfer_id: restore.transfer_id_floor }); wiring.to_transfer(TransferInput::Restored { uploads: stored, outcomes: restore.transfer_outcomes, orphans: restore.orphans, batches: restore.batches, held_orphan: restore.held_orphan }); },
                                Err(error) => {
                                    wiring.restore_failed_for(Peer::Transfer, error.to_string());
                                    wiring.to_transfer(TransferInput::RestoreFailed { error: error.to_string() });
                                }
                            }
                        }
                    }
                }
                if let Some(error) = self.restore_error.take() {
                    wiring.to_session(SessionInput::RestoreFailed { epoch, error });
                    return;
                }
                let Some(exchange) = &self.exchange else { return };
                match super::session_runner::SessionRunner::rows(exchange, &self.db) {
                    Ok(stored) => wiring.to_session(SessionInput::Restored { epoch, pending: stored.pending.map(|pending| if orphan_pending && !matches!(pending.owner, Some(crate::owners::session::MutateOwner::Transfer { step: crate::owners::write::upload::UploadStage::Init, .. })) { PendingCall { author: None, owner: Some(crate::owners::session::MutateOwner::Detached), ..pending } } else { pending }), answers: stored.answers, pending_lost: stored.dropped_pending, resume_answer: stored.resume_answer }),
                    Err(error) => wiring.to_session(SessionInput::RestoreFailed { epoch, error: error.to_string() }),
                }
            }
            StoreEffect::CloseReads => self.db.reads().close(),
            StoreEffect::CloseConnections => {}
            StoreEffect::StoreClosed => wiring.to_lifecycle(LifecycleInput::StoreClosed),
        }
    }
}

pub(super) struct NetworkRunner {
    pub wiring: Arc<Wiring>,
    /// The answer of `set_online` waits for `step_done`: a caller that heard it finds `Online` already queued for every owner.
    pub held_replies: Vec<(u64, bool)>,
}

impl EffectRunner<Network> for NetworkRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: NetworkEffect, _back: &Back<Network>) {
        let wiring = &self.wiring;
        match effect {
            NetworkEffect::Broadcast { online, online_seq, cable } => {
                wiring.to_windows(WindowsInput::Online { online: Online(online), online_seq });
                if let Some(cable) = cable { wiring.to_feed(FeedInput::Online { online: Online(cable), online_seq }); }
                wiring.to_outbox(OutboxInput::Online(Online(online)));
                wiring.to_transfer(TransferInput::Online(Online(online)));
                wiring.to_session(SessionInput::Online(Online(online)));
                wiring.to_lifecycle(LifecycleInput::Online(online));
                wiring.to_calls(CallsInput::Online(Online(online)));
                wiring.tell_network(&ALL_PEERS, NetworkEvent::Online { online, epoch: None });
            }
            NetworkEffect::Snapshot { owner, epoch, online, online_seq, saving } => {
                if wiring.epoch_passed(epoch) {
                    return;
                }
                let stamp = EpochStamp::Epoch(epoch);
                if let (Peer::Windows, Some(saving)) = (owner, saving) {
                    wiring.to_windows_in(epoch, WindowsInput::PowerSaving { saving });
                }
                match owner {
                    Peer::Windows => wiring.to_windows_in(epoch, WindowsInput::Online { online: Online(online), online_seq }),
                    Peer::Feed => wiring.to_feed(FeedInput::Online { online: Online(online), online_seq }),
                    Peer::Calls => wiring.to_calls_at(stamp, CallsInput::Snapshot(Online(online))),
                    Peer::Outbox => {
                        wiring.to_outbox_at(stamp, OutboxInput::Online(Online(online)));
                    }
                    Peer::Transfer => {
                        if let Some(saving) = saving {
                            wiring.to_transfer_at(stamp, TransferInput::PowerSaving(saving));
                        }
                        wiring.to_transfer_at(stamp, TransferInput::Online(Online(online)));
                    }
                    Peer::Session => wiring.to_session_at(stamp, SessionInput::Snapshot { epoch, online }),
                    _ => {}
                }
                wiring.tell_network(&[owner], NetworkEvent::Online { online, epoch: Some(epoch) });
                if let Some(saving) = saving {
                    wiring.tell_network(&[owner], NetworkEvent::PowerSaving { saving, epoch: Some(epoch) });
                }
            }
            NetworkEffect::Foreground { seq, online_seq } => {
                wiring.to_windows(WindowsInput::Foreground { seq, online_seq });
                wiring.to_rounds(RoundsInput::Foreground);
                wiring.to_lifecycle(LifecycleInput::Foreground);
                wiring.tell_network(&[Peer::Rounds, Peer::Session, Peer::Windows], NetworkEvent::Foreground(seq));
            }
            NetworkEffect::Cable { cable, online_seq } => wiring.to_feed(FeedInput::Online { online: Online(cable), online_seq }),
            NetworkEffect::PowerSaving(saving) => {
                wiring.to_windows(WindowsInput::PowerSaving { saving });
                wiring.to_transfer(TransferInput::PowerSaving(saving));
                wiring.tell_network(&[Peer::Transfer, Peer::Windows], NetworkEvent::PowerSaving { saving, epoch: None });
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

