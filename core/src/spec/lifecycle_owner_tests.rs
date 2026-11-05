//! Owner-level tests of the `lifecycle` machine (Lifecycle): the owner on `DetScheduler` and the reference table (`ref_lifecycle.rs`)
//! get the same input, after each one the effects, the replies, the state, `Published` and the open publications must be equal. Names are the `Test` column.

use std::collections::BTreeSet;

use super::ref_lifecycle::{Answer, Cause as RefCause, Effect as RefEffect, Erase as RefErase, EraseKind as RefEraseKind, Event, Lifecycle as Ref, Owner as RefOwner, PubState, Published as RefPublished, State as RefState, OWNERS};
use crate::owners::contract::{CredentialCommitted, Drained, PublishedState, ServerEnded};
use crate::owners::lifecycle::{Erase, EraseKind, Lifecycle, LifecycleEffect, LifecycleError, LifecycleInput, LifecyclePhase, LifecycleState, Peer};
use crate::owners::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};

fn cause(cause: Cause) -> RefCause {
    match cause {
        Cause::SessionReset => RefCause::SessionReset,
        Cause::ServerEnded => RefCause::ServerEnded,
        Cause::IdentityChanged => RefCause::IdentityChanged,
        Cause::SessionStarted => RefCause::SessionStarted,
        Cause::Close => RefCause::Close,
    }
}

fn peer(owner: RefOwner) -> &'static str {
    match owner {
        RefOwner::Network => "network",
        RefOwner::Session => "session",
        RefOwner::Window => "windows",
        RefOwner::Feed => "feed",
        RefOwner::Delivery => "deliveries",
        RefOwner::Outbox => "outbox",
        RefOwner::Transfer => "transfer",
        RefOwner::Calls => "calls",
        RefOwner::LocalWrites => "local_writes",
        RefOwner::Rounds => "rounds",
    }
}

fn peer_of(owner: RefOwner) -> Peer {
    match owner {
        RefOwner::Network => Peer::Network,
        RefOwner::Session => Peer::Session,
        RefOwner::Window => Peer::Windows,
        RefOwner::Feed => Peer::Feed,
        RefOwner::Delivery => Peer::Deliveries,
        RefOwner::Outbox => Peer::Outbox,
        RefOwner::Transfer => Peer::Transfer,
        RefOwner::Calls => Peer::Calls,
        RefOwner::LocalWrites => Peer::LocalWrites,
        RefOwner::Rounds => Peer::Rounds,
    }
}

fn ref_owner(peer: Peer) -> RefOwner {
    match peer {
        Peer::Network => RefOwner::Network,
        Peer::Session => RefOwner::Session,
        Peer::Windows => RefOwner::Window,
        Peer::Feed => RefOwner::Feed,
        Peer::Deliveries => RefOwner::Delivery,
        Peer::Outbox => RefOwner::Outbox,
        Peer::Transfer => RefOwner::Transfer,
        Peer::Calls => RefOwner::Calls,
        Peer::LocalWrites => RefOwner::LocalWrites,
        Peer::Rounds => RefOwner::Rounds,
    }
}

fn pub_state(state: PublishedState) -> PubState {
    match state {
        PublishedState::NoSession => PubState::NoSession,
        PublishedState::Session => PubState::Session,
        PublishedState::Ending => PubState::Ending,
        PublishedState::EndingFailed => PubState::EndingFailed,
        PublishedState::Resetting => PubState::Resetting,
        PublishedState::StorageFailed => PubState::StorageFailed,
        PublishedState::Closing => PubState::Closing,
    }
}

fn published(published: &crate::owners::contract::Published) -> RefPublished {
    RefPublished { epoch: published.epoch, identity: published.identity, generation: published.generation, state: pub_state(published.state), closed: published.closed }
}

fn erases_of(erases: &[Erase]) -> Vec<RefErase> {
    erases.iter().map(|erase| RefErase { id: erase.id, target: erase.target, kind: match erase.kind { EraseKind::Account => RefEraseKind::Account, EraseKind::Models => RefEraseKind::Models }, replies: erase.replies.clone() }).collect()
}

fn state_of(phase: &LifecyclePhase) -> RefState {
    match phase {
        LifecyclePhase::NoSession { epoch } => RefState::NoSession { epoch: *epoch },
        LifecyclePhase::Session { epoch, identity } => RefState::Session { epoch: *epoch, identity: *identity },
        LifecyclePhase::Ending { epoch_old, cause: c, next, failed, erase_credential, erases } => RefState::Ending { epoch_old: *epoch_old, cause: cause(*c), next: *next, failed: *failed, erase_credential: *erase_credential, erases: erases_of(erases) },
        LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases } => RefState::Resetting { epoch: *epoch, identity: *identity, models: models.clone(), queued_models: queued_models.clone(), queued_replies: queued_replies.clone(), pending_credential: *pending_credential, erases: erases_of(erases) },
        LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped } => RefState::Closing { waiting: waiting.iter().map(|p| ref_owner(*p)).collect(), erases: erases_of(erases), close_replies: close_replies.clone(), store_closing: *store_closing, store_closed: *store_closed, observers_stopped: *observers_stopped },
        LifecyclePhase::Closed => RefState::Closed,
    }
}

fn effect_of(effect: &LifecycleEffect) -> Option<RefEffect> {
    Some(match effect {
        LifecycleEffect::Publish { publication_id, published: p } => RefEffect::Publish(*publication_id, published(p)),
        LifecycleEffect::PublishCell(p) => RefEffect::PublishCell(published(p)),
        LifecycleEffect::EpochStarted { epoch, identity, generation } => RefEffect::EpochStarted(*epoch, *identity, *generation),
        LifecycleEffect::EpochEnded { epoch, cause: c } => RefEffect::EpochEnded(*epoch, cause(*c)),
        LifecycleEffect::ModelsReset { epoch, generation, models } => RefEffect::ModelsReset(*epoch, *generation, models.clone()),
        LifecycleEffect::Closing => RefEffect::Closing,
        LifecycleEffect::StopObserving => RefEffect::StopObserving,
        LifecycleEffect::ClearAccount { epoch_old, cause: c, generation_new, erase_id } => RefEffect::ClearAccount { epoch_old: *epoch_old, cause: cause(*c), generation_new: *generation_new, erase_id: *erase_id },
        LifecycleEffect::ClearModels { epoch, generation_new, models, erase_id } => RefEffect::ClearModels { epoch: *epoch, generation_new: *generation_new, models: models.clone(), erase_id: *erase_id },
        LifecycleEffect::ReadIntents { epoch } => RefEffect::ReadIntents(*epoch),
        LifecycleEffect::RestoreReady { epoch } => RefEffect::RestoreReady(*epoch),
        LifecycleEffect::StoreClose => RefEffect::StoreClose,
        LifecycleEffect::Reply { .. } => return None,
    })
}

fn models(names: &[&str]) -> BTreeSet<String> { names.iter().map(|name| name.to_string()).collect() }

pub(super) const SQL: &str = "disk";

/// The owner and the reference side by side.
pub(super) struct Twin {
    det: DetScheduler,
    addr: Address<Lifecycle>,
    log: Log<LifecycleEffect>,
    reference: Ref,
    /// Every effect of the owner since the start, for the properties.
    pub history: Vec<LifecycleEffect>,
    /// Each input with the effects it gave.
    pub steps: Vec<(Event, Vec<LifecycleEffect>)>,
}

impl Twin {
    /// Both sides start at `Open` without a session, the first publication saved.
    pub fn new() -> Self {
        let mut twin = Self::open(0, 0, None, Some(1_000));
        twin.save_all();
        twin
    }

    pub fn open(saved_epoch: u64, saved_generation: u64, saved_identity: Option<u64>, reply: Option<u64>) -> Self {
        let mut det = DetScheduler::new(7);
        let log = new_log();
        let addr = det.add::<Lifecycle>("lifecycle", LifecycleState::new(), Recorder::<Lifecycle>::new(&log));
        let (reference, want) = Ref::open(saved_epoch, saved_generation, saved_identity, reply);
        let mut twin = Self { det, addr, log, reference, history: Vec::new(), steps: Vec::new() };
        twin.send(LifecycleInput::Open { saved_epoch, saved_generation, saved_identity, reply });
        let (got, _) = twin.drain();
        assert_eq!(got, want.effects, "the open row gives the same effects");
        twin.same_state();
        twin
    }

    fn send(&mut self, input: LifecycleInput) {
        self.det.post(self.addr, EpochStamp::Control, input);
        self.det.run_until_quiet(1_000).expect("the owner is quiet");
    }

    fn drain(&mut self) -> (Vec<RefEffect>, Vec<(u64, Answer)>) {
        let raw = take(&self.log);
        self.history.extend(raw.iter().cloned());
        let replies = raw.iter().filter_map(|effect| match effect {
            LifecycleEffect::Reply { id, result } => Some((*id, match result {
                Ok(()) => Answer::Ok,
                Err(LifecycleError::Storage) => Answer::Storage,
                Err(LifecycleError::Closed) => Answer::Closed,
                Err(LifecycleError::Runtime) => Answer::Runtime,
                Err(LifecycleError::Interrupted) => Answer::Interrupted,
            })),
            _ => None,
        }).collect();
        (raw.iter().filter_map(effect_of).collect(), replies)
    }

    fn same_state(&self) {
        let state = self.det.state(self.addr).expect("the owner state");
        assert_eq!(state_of(state.phase()), self.reference.state, "the same state");
        assert_eq!(published(state.published()), self.reference.published, "the same Published");
        assert_eq!(published(state.cell()), self.reference.cell, "the same PublishedCell");
        let restoring = state.restoring().map(|r| (r.epoch, r.start_waiting.iter().map(|p| ref_owner(*p)).collect::<BTreeSet<_>>(), r.restore_waiting.iter().map(|p| ref_owner(*p)).collect::<BTreeSet<_>>(), r.open_reply));
        let wanted_restoring = self.reference.restoring.as_ref().map(|r| (r.epoch, r.start_waiting.clone(), r.restore_waiting.clone(), r.open_reply));
        assert_eq!(restoring, wanted_restoring, "the same restoring");
        let publishing: Vec<_> = state.publishing().iter().map(|p| (p.id, p.replies.clone(), p.started.as_ref().map(|s| (s.epoch, s.identity, s.generation)))).collect();
        let wanted: Vec<_> = self.reference.publishing.iter().map(|p| (p.id, p.replies.clone(), p.epoch_started)).collect();
        assert_eq!(publishing, wanted, "the same open publications");
    }

    pub fn state(&self) -> &RefState { &self.reference.state }

    pub fn published(&self) -> &RefPublished { &self.reference.published }

    /// The erases in the Store without an answer, oldest first.
    pub fn erases(&self) -> Vec<RefErase> {
        match &self.reference.state {
            RefState::Ending { erases, .. } | RefState::Resetting { erases, .. } | RefState::Closing { erases, .. } => erases.clone(),
            _ => Vec::new(),
        }
    }

    /// The id of the oldest erase of the Store.
    pub fn erase(&self) -> u64 { self.erases().first().expect("an erase is open").id }


    pub fn publications(&self) -> Vec<u64> { self.reference.publishing.iter().map(|p| p.id).collect() }

    pub fn cell(&self) -> &RefPublished { &self.reference.cell }

    /// The Store saves every publication, then Outbox and Transfer start and the three receivers classify the restored intents.
    pub fn settle(&mut self) {
        for _ in 0..6 {
            self.save_all();
            let Some(restoring) = self.reference.restoring.clone() else { break };
            let epoch = restoring.epoch;
            if !restoring.start_waiting.is_empty() {
                restoring.start_waiting.iter().for_each(|owner| { self.feed(Event::Started(*owner, epoch)); });
            } else {
                restoring.restore_waiting.iter().for_each(|owner| { self.feed(Event::RestoreClassified(*owner, epoch)); });
            }
        }
    }

    /// The Store answers `Saved` for every open publication, oldest first.
    pub fn save_all(&mut self) {
        for id in self.publications() {
            self.feed(Event::Saved(id));
        }
    }

    /// Feeds one input of the table to both sides and compares everything. Returns the effects of the reference.
    pub fn feed(&mut self, event: Event) -> Vec<RefEffect> {
        let epoch_old = match &self.reference.state {
            RefState::Ending { epoch_old, .. } => *epoch_old,
            _ => 0,
        };
        let _ = epoch_old;
        let input = match event.clone() {
            Event::Credential(identity) => LifecycleInput::CredentialCommitted(CredentialCommitted { identity }),
            Event::SessionReset(reply) => LifecycleInput::SessionReset { reply },
            Event::ServerEnded(epoch) => LifecycleInput::ServerEnded(ServerEnded { epoch }),
            Event::ResetModels(names, reply) => LifecycleInput::ResetModels { models: names, reply },
            Event::Cleared(erase_id) => LifecycleInput::Cleared { erase_id },
            Event::ModelsCleared(generation_new, erase_id) => LifecycleInput::ModelsCleared { generation_new, erase_id },
            Event::ClearFailed(erase_id) => LifecycleInput::ClearFailed { erase_id, error: SQL.into() },
            Event::Saved(publication_id) => LifecycleInput::Saved { publication_id },
            Event::SaveFailed(publication_id) => LifecycleInput::SaveFailed { publication_id, error: SQL.into() },
            Event::StoreClosed => LifecycleInput::StoreClosed,
            Event::ObserversStopped => LifecycleInput::ObserversStopped,
            Event::ExecutorFailed => LifecycleInput::ExecutorFailed { executor: "store".into() },
            Event::Started(owner, epoch) => LifecycleInput::Started { owner: peer_of(owner), epoch },
            Event::RestoreClassified(owner, epoch) => LifecycleInput::RestoreClassified { owner: peer_of(owner), epoch },
            Event::Close(reply) => LifecycleInput::Close { reply },
            Event::Drained(owner) => LifecycleInput::Drained(Drained { owner: peer(owner) }),
        };
        self.send(input);
        let before = self.history.len();
        let (got, replies) = self.drain();
        self.steps.push((event.clone(), self.history[before..].to_vec()));
        let want = self.reference.step(event);
        assert_eq!(got, want.effects, "the owner and the reference give the same effects");
        assert_eq!(replies, want.replies, "the owner and the reference give the same replies");
        self.same_state();
        want.effects
    }

    pub fn feed_all(&mut self, events: impl IntoIterator<Item = Event>) { events.into_iter().for_each(|event| { self.feed(event); }); }

    /// The reference reply list of the last input is not kept: tests read the replies from the history.
    pub fn replies(&self) -> Vec<(u64, Result<(), LifecycleError>)> {
        self.history.iter().filter_map(|effect| match effect { LifecycleEffect::Reply { id, result } if *id != 1_000 => Some((*id, *result)), _ => None }).collect()
    }
}

fn credential(identity: u64) -> Event { Event::Credential(identity) }
fn reset(reply: u64) -> Event { Event::SessionReset(Some(reply)) }
fn reset_models(names: &[&str], reply: u64) -> Event { Event::ResetModels(models(names), Some(reply)) }
fn close(reply: u64) -> Event { Event::Close(reply) }
fn drain_all() -> Vec<Event> { OWNERS.into_iter().map(Event::Drained).collect() }

/// Session 1 with the start saved.
fn session(identity: u64) -> Twin {
    let mut twin = Twin::new();
    twin.feed(credential(identity));
    twin.settle();
    twin
}
/// Session 1, `SessionReset` issued, the Store has not answered.
fn ending() -> Twin {
    let mut twin = session(1);
    twin.feed(reset(1));
    twin.settle();
    twin
}
/// Session 1, `ResetModels` issued, the Store has not answered.
fn resetting() -> Twin {
    let mut twin = session(1);
    twin.feed(reset_models(&["Moment"], 1));
    twin.settle();
    twin
}
/// NoSession, `ResetModels` issued.
fn resetting_signed_out() -> Twin {
    let mut twin = Twin::new();
    twin.feed(reset_models(&["Moment"], 1));
    twin.settle();
    twin
}
fn closing_session() -> Twin {
    let mut twin = session(1);
    twin.feed(close(1));
    twin.settle();
    twin
}
fn stray_erase_events() -> [Event; 3] { [Event::Cleared(999), Event::ModelsCleared(9, 999), Event::ClearFailed(999)] }
/// `Cleared` for the oldest erase.
fn cleared(twin: &Twin) -> Event { Event::Cleared(twin.erase()) }
fn models_cleared(twin: &Twin, generation: u64) -> Event { Event::ModelsCleared(generation, twin.erase()) }
fn clear_failed(twin: &Twin) -> Event { Event::ClearFailed(twin.erase()) }

#[test]
fn life_open_no_session() {
    let mut twin = Twin::open(4, 2, None, Some(7));
    assert_eq!(*twin.published(), RefPublished { epoch: 5, identity: None, generation: 2, state: PubState::NoSession, closed: false });
    assert!(twin.replies().is_empty(), "open is answered after Saved");
    let effects = twin.feed(Event::Saved(1));
    assert_eq!(effects, vec![RefEffect::PublishCell(twin.published().clone()), RefEffect::EpochStarted(5, None, 2)]);
    assert_eq!(twin.replies(), vec![(7, Ok(()))]);
}

#[test]
fn life_open_session() {
    let mut twin = Twin::open(4, 2, Some(9), Some(7));
    assert_eq!(*twin.published(), RefPublished { epoch: 5, identity: Some(9), generation: 2, state: PubState::Session, closed: false });
    assert_eq!(*twin.cell(), RefPublished { epoch: 0, identity: None, generation: 0, state: PubState::NoSession, closed: false }, "an active publication is visible after Saved");
    let effects = twin.feed(Event::Saved(1));
    assert_eq!(effects, vec![RefEffect::PublishCell(twin.published().clone()), RefEffect::EpochStarted(5, Some(9), 2)]);
    assert!(twin.replies().is_empty(), "open waits for the receivers of the snapshot");
    twin.feed(Event::Started(RefOwner::Outbox, 5));
    let read = twin.feed(Event::Started(RefOwner::Transfer, 5));
    assert_eq!(read, vec![RefEffect::ReadIntents(5)], "the intents are read after Saved and Started of both receivers");
    twin.feed(Event::RestoreClassified(RefOwner::Outbox, 5));
    twin.feed(Event::RestoreClassified(RefOwner::Transfer, 5));
    assert!(twin.replies().is_empty());
    let ready = twin.feed(Event::RestoreClassified(RefOwner::Session, 5));
    assert_eq!(ready, vec![RefEffect::RestoreReady(5)]);
    assert_eq!(twin.replies(), vec![(7, Ok(()))]);
}

#[test]
fn life_nosession_credential() { session(1); }

#[test]
fn life_nosession_reset() { Twin::new().feed(reset(1)); }

#[test]
fn life_nosession_server_ended() { Twin::new().feed(Event::ServerEnded(1)); }

#[test]
fn life_nosession_reset_models() { resetting_signed_out(); }

#[test]
fn life_nosession_stray_store() { Twin::new().feed_all(stray_erase_events()); }

#[test]
fn life_nosession_close() { Twin::new().feed(close(1)); }

#[test]
fn life_nosession_drained() { Twin::new().feed(Event::Drained(RefOwner::Feed)); }

#[test]
fn life_session_same_credential() { session(1).feed(credential(1)); }

#[test]
fn life_session_identity_changed() { session(1).feed(credential(2)); }

#[test]
fn life_session_reset() { ending(); }

#[test]
fn life_session_server_ended() { session(1).feed(Event::ServerEnded(2)); }

#[test]
fn life_session_stale_server_ended() { session(1).feed(Event::ServerEnded(1)); }

#[test]
fn life_session_reset_models() { resetting(); }

#[test]
fn life_session_stray_store() { session(1).feed_all(stray_erase_events()); }

#[test]
fn life_session_close() { closing_session(); }

#[test]
fn life_session_drained() { session(1).feed(Event::Drained(RefOwner::Calls)); }

#[test]
fn life_ending_credential() { ending().feed(credential(2)); }

#[test]
fn life_ending_credential_after_failure() {
    let mut twin = ending();
    let failed = clear_failed(&twin);
    twin.feed(failed);
    let effects = twin.feed(credential(2));
    assert!(effects.iter().any(|e| matches!(e, RefEffect::ClearAccount { .. })), "the clearing repeats");
    twin.settle();
    let done = cleared(&twin);
    twin.feed(done);
    twin.settle();
    assert_eq!(twin.published().identity, Some(2), "the committed credential is not lost");
}

#[test]
fn life_ending_reset_joins() { ending().feed(reset(2)); }

#[test]
fn life_ending_reset_erases_credential() {
    let mut twin = session(1);
    twin.feed(credential(2));
    twin.settle();
    twin.feed(reset(5));
    let first = cleared(&twin);
    let effects = twin.feed(first);
    assert!(effects.iter().any(|e| matches!(e, RefEffect::ClearAccount { cause: RefCause::SessionReset, .. })), "the credential is erased by the second clearing");
    twin.settle();
    let second = cleared(&twin);
    twin.feed(second);
    twin.settle();
    assert_eq!(twin.published().identity, None);
    assert!(twin.replies().contains(&(5, Ok(()))));
}

#[test]
fn life_ending_reset_after_failure() {
    let mut twin = ending();
    let failed = clear_failed(&twin);
    twin.feed(failed);
    twin.feed(reset(2));
}

#[test]
fn life_ending_server_ended() { ending().feed(Event::ServerEnded(2)); }

#[test]
fn life_ending_reset_models() { ending().feed(reset_models(&["Moment"], 2)); }

#[test]
fn life_ending_failed_reset_models() {
    let mut twin = ending();
    let failed = clear_failed(&twin);
    twin.feed(failed);
    twin.feed(reset_models(&["Moment"], 2));
    assert!(twin.replies().contains(&(2, Err(LifecycleError::Storage))));
}

#[test]
fn life_ending_cleared_then_credential() {
    let mut twin = session(1);
    twin.feed(credential(2));
    twin.feed(reset(5));
    let first = cleared(&twin);
    twin.feed(first);
}

#[test]
fn life_ending_cleared_signed_out() {
    let mut twin = ending();
    let done = cleared(&twin);
    twin.feed(done);
    assert!(twin.replies().is_empty(), "the reset is answered after Saved");
    twin.settle();
    assert_eq!(twin.replies(), vec![(1, Ok(()))]);
}

#[test]
fn life_ending_cleared_next_identity() {
    let mut twin = session(1);
    twin.feed(credential(2));
    twin.settle();
    let done = cleared(&twin);
    twin.feed(done);
    assert_eq!(*twin.state(), RefState::Session { epoch: 4, identity: 2 });
}

#[test]
fn life_ending_clear_failed() {
    let mut twin = ending();
    let failed = clear_failed(&twin);
    twin.feed(failed);
    assert_eq!(twin.published().state, PubState::EndingFailed);
    assert!(twin.replies().contains(&(1, Err(LifecycleError::Storage))));
}

#[test]
fn life_ending_models_cleared() {
    let mut twin = resetting();
    twin.feed(reset(2));
    let models = twin.erases().into_iter().find(|erase| erase.kind == RefEraseKind::Models).expect("the models erase is open").id;
    twin.feed(Event::ModelsCleared(1, models));
    assert!(twin.replies().contains(&(1, Ok(()))));
}

#[test]
fn life_ending_models_clear_failed() {
    let mut twin = resetting();
    twin.feed(reset(2));
    let models = twin.erases().into_iter().find(|erase| erase.kind == RefEraseKind::Models).expect("the models erase is open").id;
    twin.feed(Event::ClearFailed(models));
    assert!(twin.replies().contains(&(1, Err(LifecycleError::Storage))));
}

#[test]
fn life_ending_stray_erase() { ending().feed_all(stray_erase_events()); }

#[test]
fn life_ending_close() { ending().feed(close(2)); }

#[test]
fn life_ending_drained() { ending().feed(Event::Drained(RefOwner::Feed)); }

#[test]
fn life_resetting_credential() { resetting().feed(credential(2)); }

#[test]
fn life_resetting_credential_signed_out() { resetting_signed_out().feed(credential(2)); }

#[test]
fn life_resetting_same_credential() { resetting().feed(credential(1)); }

#[test]
fn life_resetting_session_reset() { resetting().feed(reset(2)); }

#[test]
fn life_resetting_server_ended() { resetting().feed(Event::ServerEnded(2)); }

#[test]
fn life_resetting_stale_server_ended() {
    resetting().feed(Event::ServerEnded(1));
    resetting_signed_out().feed(Event::ServerEnded(1));
}

#[test]
fn life_resetting_reset_models() { resetting().feed(reset_models(&["Other"], 2)); }

#[test]
fn life_resetting_cleared_queued() {
    let mut twin = resetting();
    twin.feed(reset_models(&["Other"], 2));
    let first = models_cleared(&twin, 1);
    let effects = twin.feed(first);
    assert!(effects.iter().any(|e| matches!(e, RefEffect::ClearModels { generation_new: 2, .. })), "the queued models are erased next");
    twin.settle();
    let second = models_cleared(&twin, 2);
    twin.feed(second);
    twin.settle();
    assert_eq!(twin.replies(), vec![(1, Ok(())), (2, Ok(()))]);
}

#[test]
fn life_resetting_cleared() {
    let mut twin = resetting();
    let done = models_cleared(&twin, 1);
    twin.feed(done);
    twin.settle();
    assert_eq!(twin.published().generation, 1);
    assert_eq!(*twin.state(), RefState::Session { epoch: 2, identity: 1 });
}

#[test]
fn life_resetting_cleared_then_start() {
    let mut twin = resetting_signed_out();
    twin.feed(credential(2));
    let done = models_cleared(&twin, 1);
    twin.feed(done);
    assert_eq!(*twin.state(), RefState::Session { epoch: 2, identity: 2 });
}

#[test]
fn life_resetting_clear_failed() {
    let mut twin = resetting();
    let failed = clear_failed(&twin);
    twin.feed(failed);
    assert_eq!(twin.published().generation, 0, "the generation stays");
    assert!(twin.replies().contains(&(1, Err(LifecycleError::Storage))));
}

#[test]
fn life_resetting_clear_failed_then_start() {
    let mut twin = resetting_signed_out();
    twin.feed(credential(2));
    let failed = clear_failed(&twin);
    twin.feed(failed);
    assert_eq!(twin.published().identity, Some(2), "the committed credential is not lost");
}

#[test]
fn life_resetting_stray_erase() { resetting().feed_all(stray_erase_events()); }

#[test]
fn life_resetting_close() {
    let mut twin = resetting();
    twin.feed(reset_models(&["Other"], 2));
    twin.feed(close(3));
    assert!(twin.replies().contains(&(2, Err(LifecycleError::Interrupted))));
}

/// life_resetting_credential_open_saved: the epoch of an `open` that passed `Saved` ends with a new identity, `open` answers `Ok`.
#[test]
fn life_resetting_credential_open_saved() {
    let mut twin = Twin::open(0, 0, Some(1), Some(7));
    twin.feed(Event::Saved(1));
    twin.feed(reset_models(&["Moment"], 8));
    twin.feed(credential(2));
    assert!(twin.replies().contains(&(7, Ok(()))), "{:?}", twin.replies());
}

/// life_session_credential_open_unsaved: the first publication of `open` is not saved when the identity changes, `open` is interrupted.
#[test]
fn life_session_credential_open_unsaved() {
    let mut twin = Twin::open(0, 0, Some(1), Some(7));
    twin.feed(credential(2));
    assert!(twin.replies().contains(&(7, Err(LifecycleError::Interrupted))), "{:?}", twin.replies());
}

#[test]
fn life_resetting_drained() { resetting().feed(Event::Drained(RefOwner::Feed)); }

#[test]
fn life_closing_last_drained() {
    let mut twin = closing_session();
    let owners = OWNERS;
    for owner in owners {
        twin.feed(Event::Drained(owner));
    }
    assert!(matches!(twin.state(), RefState::Closing { store_closing: true, .. }));
    assert!(twin.feed(Event::Drained(RefOwner::Feed)).is_empty(), "a repeated Drained closes the store a second time");
}

#[test]
fn life_closing_drained() { closing_session().feed(Event::Drained(RefOwner::Feed)); }

#[test]
fn life_closing_clear_done_last() {
    let mut twin = ending();
    twin.feed(close(2));
    twin.feed_all(drain_all());
    let done = cleared(&twin);
    let effects = twin.feed(done);
    assert!(effects.contains(&RefEffect::StoreClose), "the store closes after the last erase");
}

#[test]
fn life_closing_clear_done() {
    let mut twin = ending();
    twin.feed(close(2));
    let done = cleared(&twin);
    twin.feed(done);
    assert!(twin.replies().contains(&(1, Ok(()))), "the wait of the erase is answered by its own outcome");
}

#[test]
fn life_closing_stray_erase() { closing_session().feed_all(stray_erase_events()); }

#[test]
fn life_closing_store_closed() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    twin.feed(Event::ObserversStopped);
    twin.feed(Event::StoreClosed);
    assert_eq!(*twin.state(), RefState::Closed);
    assert!(twin.replies().contains(&(1, Ok(()))));
}

#[test]
fn life_closing_store_closed_waits_observers() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    twin.feed(Event::StoreClosed);
    assert!(matches!(twin.state(), RefState::Closing { store_closed: true, .. }));
}

#[test]
fn life_closing_observers_stopped_last() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    twin.feed(Event::StoreClosed);
    twin.feed(Event::ObserversStopped);
    assert_eq!(*twin.state(), RefState::Closed);
}

#[test]
fn life_closing_observers_stopped() {
    let mut twin = closing_session();
    twin.feed(Event::ObserversStopped);
    assert!(matches!(twin.state(), RefState::Closing { observers_stopped: true, .. }));
}

#[test]
fn life_closing_close_joins() { closing_session().feed(close(2)); }

#[test]
fn life_closing_saved() {
    let mut twin = session(1);
    twin.feed(reset(2));
    twin.feed(close(3));
    let erase = cleared(&twin);
    twin.feed(erase);
    twin.settle();
    assert!(twin.replies().contains(&(2, Ok(()))));
}

#[test]
fn life_closing_refuses() {
    let mut twin = closing_session();
    twin.feed(reset(5));
    twin.feed(reset_models(&["Moment"], 6));
    twin.feed_all([credential(2), Event::ServerEnded(2)]);
    assert!(twin.replies().contains(&(5, Err(LifecycleError::Closed))));
    assert!(twin.replies().contains(&(6, Err(LifecycleError::Closed))));
}

#[test]
fn life_closing_saved_stray() { closing_session().feed_all([Event::Saved(999), Event::SaveFailed(999)]); }

#[test]
fn life_saved() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    let effects = twin.feed(Event::Saved(1));
    assert!(effects.contains(&RefEffect::EpochStarted(1, None, 0)));
    assert_eq!(twin.replies(), vec![(7, Ok(()))]);
    assert_eq!(*twin.cell(), *twin.published());
}

#[test]
fn life_saved_stale() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(credential(1));
    twin.save_all();
    assert!(twin.replies().contains(&(7, Err(LifecycleError::Interrupted))), "the publication of an ended epoch answers Interrupted once");
    assert!(!twin.history.iter().any(|e| matches!(e, LifecycleEffect::EpochStarted { epoch: 1, .. })), "EpochStarted of an ended epoch does not go out");
}

#[test]
fn life_saved_stray() { session(1).feed_all([Event::Saved(999), Event::SaveFailed(999)]); }

#[test]
fn life_save_failed() {
    for saved_identity in [None, Some(9)] {
        let mut twin = Twin::open(0, 0, saved_identity, Some(7));
        twin.feed(Event::SaveFailed(1));
        assert_eq!(twin.cell().state, PubState::StorageFailed, "{saved_identity:?}");
        assert_eq!(twin.replies(), vec![(7, Err(LifecycleError::Storage))], "{saved_identity:?}: open answers Err(Storage) once");
        assert!(!twin.history.iter().any(|e| matches!(e, LifecycleEffect::EpochStarted { .. } | LifecycleEffect::ReadIntents { .. })), "{saved_identity:?}: no work of the epoch starts");
    }
}

#[test]
fn life_save_failed_open() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.feed(Event::SaveFailed(1));
    twin.feed_all([Event::Started(RefOwner::Outbox, 1), Event::Started(RefOwner::Transfer, 1), Event::RestoreClassified(RefOwner::Session, 1)]);
    assert_eq!(twin.replies(), vec![(7, Err(LifecycleError::Storage))], "a late restore answer does not answer open again");
}

#[test]
fn life_save_failed_stale() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(credential(1));
    twin.feed(Event::Saved(2));
    let before = twin.cell().clone();
    twin.feed(Event::SaveFailed(1));
    assert_eq!(*twin.cell(), before, "the later publication stays");
    assert!(twin.replies().contains(&(7, Err(LifecycleError::Storage))));
}

#[test]
fn life_owner_started() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.feed(Event::Saved(1));
    assert!(twin.feed(Event::Started(RefOwner::Outbox, 1)).is_empty());
}

#[test]
fn life_owners_started_read_intents() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.feed(Event::Saved(1));
    twin.feed(Event::Started(RefOwner::Outbox, 1));
    assert_eq!(twin.feed(Event::Started(RefOwner::Transfer, 1)), vec![RefEffect::ReadIntents(1)]);
}

#[test]
fn life_restore_classified() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.feed(Event::Saved(1));
    assert!(twin.feed(Event::RestoreClassified(RefOwner::Session, 1)).is_empty());
}

#[test]
fn life_restore_classified_last() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.settle();
    assert_eq!(twin.replies(), vec![(7, Ok(()))]);
}

#[test]
fn life_restore_stray() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.feed(Event::Saved(1));
    twin.feed_all([Event::Started(RefOwner::Outbox, 99), Event::RestoreClassified(RefOwner::Window, 1), Event::Started(RefOwner::Calls, 1)]);
    twin.feed(close(3));
    twin.feed(Event::RestoreClassified(RefOwner::Outbox, 1));
}

#[test]
fn life_executor_failed() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(Event::ExecutorFailed);
    assert!(twin.replies().contains(&(7, Err(LifecycleError::Runtime))), "the open wait gets Runtime");
    assert!(twin.published().closed);
}

#[test]
fn life_closed_refuses() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    twin.feed_all([Event::ObserversStopped, Event::StoreClosed]);
    twin.feed(reset(5));
    twin.feed(reset_models(&["Moment"], 6));
    twin.feed(close(7));
    twin.feed_all([Event::Saved(999), Event::ExecutorFailed]);
    for id in [5, 6, 7] {
        assert!(twin.replies().contains(&(id, Err(LifecycleError::Closed))));
    }
}

#[test]
fn life_call_ending() {
    let twin = ending();
    assert_eq!(twin.published().state, PubState::Ending);
    assert_eq!(twin.published().epoch, 2, "the calls of the ended epoch stay out");
}

#[test]
fn life_call_storage_failed() {
    let mut twin = Twin::new();
    twin.feed(credential(1));
    let publication = twin.publications()[0];
    twin.feed(Event::SaveFailed(publication));
    assert_eq!(twin.cell().state, PubState::StorageFailed);
}

#[test]
fn life_wait_session_reset() {
    let mut twin = ending();
    let done = cleared(&twin);
    twin.feed(done);
    twin.settle();
    assert_eq!(twin.replies(), vec![(1, Ok(()))]);
}

#[test]
fn life_wait_close() {
    let mut twin = session(1);
    twin.feed(close(1));
    twin.feed_all(drain_all());
    twin.feed_all([Event::ObserversStopped, Event::StoreClosed]);
    assert_eq!(twin.replies(), vec![(1, Ok(()))]);
}

#[test]
fn life_wait_open() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.settle();
    assert_eq!(twin.replies(), vec![(7, Ok(()))]);
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

fn random_event(seed: &mut u64, step: u64, twin: &Twin) -> Event {
    let identity = 1 + next(seed) % 2;
    let erases = twin.erases();
    let erase = if erases.is_empty() || next(seed) % 5 == 0 { 999 } else { erases[(next(seed) as usize) % erases.len()].id };
    let publications = twin.publications();
    let publication = if publications.is_empty() || next(seed) % 6 == 0 { 999 } else { publications[(next(seed) as usize) % publications.len()] };
    match next(seed) % 22 {
        0 | 1 => Event::Credential(identity),
        2 => Event::SessionReset(Some(step)),
        3 => Event::ServerEnded(1 + next(seed) % 6),
        4 => Event::ResetModels(models(&["Moment"]), Some(step)),
        5 | 6 => Event::Cleared(erase),
        7 | 8 => Event::ModelsCleared(1 + next(seed) % 8, erase),
        9 => Event::ClearFailed(erase),
        10 => Event::Close(step),
        11..=14 => Event::Saved(publication),
        15 => Event::SaveFailed(publication),
        16 => Event::StoreClosed,
        17 => Event::ObserversStopped,
        18 => Event::Started(OWNERS[(next(seed) % 10) as usize], 1 + next(seed) % 4),
        19 => Event::RestoreClassified(OWNERS[(next(seed) % 10) as usize], 1 + next(seed) % 4),
        _ => Event::Drained(OWNERS[(next(seed) % 10) as usize]),
    }
}

/// A trace of `len` random inputs; the Store answers are random too.
fn trace(seed: u64, len: u64) -> Twin {
    let mut seed = seed;
    let mut twin = Twin::open(0, 0, None, Some(1_000));
    for step in 1..=len {
        let event = random_event(&mut seed, step, &twin);
        twin.feed(event);
    }
    twin
}

fn traces() -> impl Iterator<Item = Twin> { (1..=60).map(|seed| trace(seed, 40)) }

#[test]
fn life_prop_epoch_monotonic() {
    for twin in traces() {
        let mut last = 0;
        for effect in &twin.history {
            if let LifecycleEffect::Publish { published, .. } = effect {
                assert!(published.epoch >= last, "Published.epoch {} after {last}", published.epoch);
                last = published.epoch;
            }
        }
    }
}

#[test]
fn life_prop_no_work_after_end() {
    for twin in traces() {
        let mut ended: BTreeSet<u64> = BTreeSet::new();
        for effect in &twin.history {
            match effect {
                LifecycleEffect::EpochEnded { epoch, .. } => { ended.insert(*epoch); }
                LifecycleEffect::ClearModels { epoch, .. } | LifecycleEffect::ModelsReset { epoch, .. } => assert!(!ended.contains(epoch), "work in the ended epoch {epoch}"),
                LifecycleEffect::EpochStarted { epoch, .. } => assert!(!ended.contains(epoch), "the ended epoch {epoch} starts again"),
                _ => {}
            }
        }
    }
}

#[test]
fn life_prop_clear_before_start() {
    for twin in traces() {
        let mut clearing = false;
        for (event, effects) in &twin.steps {
            for effect in effects {
                match effect {
                    LifecycleEffect::ClearAccount { .. } => clearing = true,
                    LifecycleEffect::EpochStarted { identity: Some(_), .. } => assert!(!clearing || matches!(event, Event::Saved(_)), "an epoch with an identity starts before the account clearing answered"),
                    _ => {}
                }
            }
            if matches!(event, Event::Cleared(_)) {
                clearing = false;
            }
        }
    }
}

/// Every command with a reply is answered at most once; after the owners drain, the Store closes and the observers stop, `close` is answered.
#[test]
fn life_prop_reply_once() {
    for seed in 1..=60 {
        let mut twin = trace(seed, 40);
        twin.feed(close(5_000));
        twin.feed_all(drain_all());
        for erase in twin.erases() {
            twin.feed(Event::Cleared(erase.id));
        }
        twin.feed_all([Event::ObserversStopped, Event::StoreClosed]);
        let mut seen: Vec<u64> = Vec::new();
        for effect in &twin.history {
            if let LifecycleEffect::Reply { id, .. } = effect {
                if *id == 5_000 || *id == 1_000 { continue; }
                assert!(!seen.contains(id), "reply {id} comes twice");
                seen.push(*id);
            }
        }
        assert_eq!(*twin.state(), RefState::Closed, "seed {seed}");
        assert!(twin.replies().iter().any(|(id, _)| *id == 5_000), "close is answered");
    }
}

#[test]
fn life_prop_close_drains() {
    let mut twin = session(1);
    twin.feed(close(1));
    let owners = OWNERS;
    for (index, owner) in owners.into_iter().enumerate() {
        let effects = twin.feed(Event::Drained(owner));
        let last = index + 1 == owners.len();
        assert_eq!(effects.contains(&RefEffect::StoreClose), last, "close ends with the last Drained and not before");
    }
    twin.feed(Event::StoreClosed);
    assert!(!twin.replies().iter().any(|(id, _)| *id == 1), "close waits for the observers");
    twin.feed(Event::ObserversStopped);
    assert!(twin.replies().contains(&(1, Ok(()))));
    assert!(twin.published().closed);
}

#[test]
fn life_prop_generation() {
    for twin in traces() {
        let mut last = 0;
        for effect in &twin.history {
            if let LifecycleEffect::Publish { published, .. } = effect {
                assert!(published.generation >= last, "generation {} after {last}", published.generation);
                last = published.generation;
            }
        }
    }
}

/// `ClearAccount` goes to the Store with its cause: the Store erases `encrypted_*` for `SessionReset` and `ServerEnded` only.
#[test]
fn life_prop_session_kept() {
    for twin in traces() {
        let mut handed = BTreeSet::new();
        for effect in &twin.history {
            match effect {
                LifecycleEffect::ClearAccount { generation_new, erase_id, .. } | LifecycleEffect::ClearModels { generation_new, erase_id, .. } => {
                    assert!(handed.insert(*erase_id), "the erase id {erase_id} is handed twice");
                    let _ = generation_new;
                }
                _ => {}
            }
        }
    }
    let mut twin = session(1);
    let effects = twin.feed(credential(2));
    assert!(effects.iter().any(|e| matches!(e, RefEffect::ClearAccount { cause: RefCause::IdentityChanged, .. })), "a change of identity clears with IdentityChanged");
}

/// Two erases in flight never get the same generation: the Store refuses `generation_new` at or below the last.
#[test]
fn life_prop_generation_distinct() {
    for twin in traces() {
        let mut open: Vec<u64> = Vec::new();
        for (event, effects) in &twin.steps {
            for effect in effects {
                match effect {
                    LifecycleEffect::ClearAccount { generation_new, .. } | LifecycleEffect::ClearModels { generation_new, .. } => {
                        assert!(!open.contains(generation_new), "generation {generation_new} handed to two erases in flight");
                        open.push(*generation_new);
                    }
                    _ => {}
                }
            }
            if matches!(event, Event::Cleared(_) | Event::ModelsCleared(..) | Event::ClearFailed(_)) {
                open.clear();
            }
        }
    }
}

#[test]
fn life_prop_idle() {
    let mut twin = trace(3, 30);
    twin.send(LifecycleInput::Idle);
    assert!(twin.drain().0.is_empty(), "no input, no effect");
}

#[test]
fn life_prop_credential_kept() {
    for credential_id in [1, 2] {
        let mut twin = resetting_signed_out();
        twin.feed(credential(credential_id));
        let done = models_cleared(&twin, 1);
        twin.feed(done);
        assert_eq!(twin.published().identity, Some(credential_id));
    }
    let mut twin = resetting_signed_out();
    twin.feed(credential(2));
    let failed = clear_failed(&twin);
    twin.feed(failed);
    assert_eq!(twin.published().identity, Some(2));
}

#[test]
fn life_prop_every_epoch_ends() {
    for twin in traces() {
        let mut ended: BTreeSet<u64> = BTreeSet::new();
        for effect in &twin.history {
            match effect {
                LifecycleEffect::EpochEnded { epoch, .. } => { ended.insert(*epoch); }
                LifecycleEffect::EpochStarted { epoch, .. } if *epoch > 1 => assert!(ended.contains(&(epoch - 1)) || ended.contains(&(epoch - 2)), "epoch {epoch} starts, neither {} nor {} ended", epoch - 1, epoch - 2),
                _ => {}
            }
        }
    }
}

/// `EpochStarted` and the answers of a publication go out after its `Saved` only.
#[test]
fn life_prop_publish_after_saved() {
    for twin in traces() {
        let mut saved: BTreeSet<u64> = BTreeSet::new();
        let waiting: Vec<(u64, u64)> = twin.history.iter().filter_map(|effect| match effect { LifecycleEffect::Publish { publication_id, published } => Some((*publication_id, published.epoch)), _ => None }).collect();
        for (event, effects) in &twin.steps {
            if let Event::Saved(id) = event { saved.insert(*id); }
            for effect in effects {
                match effect {
                    LifecycleEffect::EpochStarted { epoch, .. } => {
                        assert!(waiting.iter().any(|(id, e)| e == epoch && saved.contains(id)), "EpochStarted({epoch}) before its Saved");
                    }
                    _ => {}
                }
            }
        }
    }
}

/// `Saved` of one publication leaves the other open publications alone.
#[test]
fn life_prop_publication_id() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(credential(1));
    let second = twin.publications()[1];
    twin.feed(Event::Saved(second));
    assert!(!twin.replies().iter().any(|(id, _)| *id == 7), "the later Saved does not answer the earlier publication");
    assert_eq!(twin.publications().len(), 1);
}

/// The traces of the findings, at the level of the owner.
#[test]
fn life_findings() {
    let mut identity = session(1);
    identity.feed(credential(2));
    let done = cleared(&identity);
    identity.feed(done);
    identity.settle();
    assert_eq!(*identity.state(), RefState::Session { epoch: 4, identity: 2 }, "a change of identity clears first, then starts");

    let mut models_reset = session(1);
    models_reset.feed(reset_models(&["Other"], 1));
    let done = models_cleared(&models_reset, 1);
    models_reset.feed(done);
    assert_eq!(models_reset.published().generation, 1);

    let mut closing_during_clear = ending();
    closing_during_clear.feed(close(2));
    closing_during_clear.feed_all(drain_all());
    let done = cleared(&closing_during_clear);
    closing_during_clear.feed(done);
    closing_during_clear.feed_all([Event::ObserversStopped, Event::StoreClosed]);
    assert_eq!(*closing_during_clear.state(), RefState::Closed);

    let mut double_reset = ending();
    double_reset.feed(reset(2));
    let done = cleared(&double_reset);
    double_reset.feed(done);
    double_reset.settle();
    assert_eq!(double_reset.history.iter().filter(|e| matches!(e, LifecycleEffect::Reply { result: Ok(()), .. })).count(), 2 + 1, "both resets are answered");

    let mut signed_out = resetting_signed_out();
    signed_out.feed(credential(2));
    let failed = clear_failed(&signed_out);
    signed_out.feed(failed);
    assert_eq!(signed_out.published().identity, Some(2));

    let mut opened = Twin::new();
    opened.feed(credential(1));
    assert!(opened.history.iter().any(|e| matches!(e, LifecycleEffect::EpochEnded { cause: Cause::SessionStarted, .. })), "a stream open without a session ends when the account is entered");
}

/// P-LIFE-12: `open` is answered exactly when the last receiver has classified, in every delivery order and with no clock.
#[test]
fn life_prop_open_no_timeout() {
    let starts = [[RefOwner::Outbox, RefOwner::Transfer], [RefOwner::Transfer, RefOwner::Outbox]];
    let classes = [RefOwner::Outbox, RefOwner::Transfer, RefOwner::Session];
    let class_orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
    for start in starts {
        for order in class_orders {
            let mut twin = Twin::open(4, 2, Some(9), Some(7));
            twin.feed(Event::Saved(1));
            for owner in start { twin.feed(Event::Started(owner, 5)); }
            for (at, index) in order.into_iter().enumerate() {
                assert!(twin.replies().is_empty(), "{start:?} {order:?}: open answered before the receiver {at} classified");
                twin.feed(Event::RestoreClassified(classes[index], 5));
            }
            assert_eq!(twin.replies(), vec![(7, Ok(()))], "{start:?} {order:?}: open is answered once all receivers classified");
        }
    }
    let mut failed = Twin::open(4, 2, Some(9), Some(7));
    failed.feed(Event::SaveFailed(1));
    assert_eq!(failed.replies(), vec![(7, Err(LifecycleError::Storage))], "a failed save answers open with Storage without waiting for anything");
}
