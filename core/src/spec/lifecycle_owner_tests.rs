//! Owner-level tests of the `lifecycle` machine (Lifecycle): the owner on `DetScheduler` and the reference table (`ref_lifecycle.rs`)
//! get the same input, after each one the effects, the replies, the state, `Published` and the open publications must be equal. Names are the `Test` column.

use std::collections::BTreeSet;

use super::ref_lifecycle::{Answer, Cause as RefCause, Effect as RefEffect, Erase as RefErase, EraseKind as RefEraseKind, Event, Lifecycle as Ref, Owner as RefOwner, PubState, Published as RefPublished, State as RefState, OWNERS};
use crate::owners::contract::{CredentialCommitted, Drained, PublishedState, ServerEnded};
use crate::owners::lifecycle::{Erase, EraseKind, Lifecycle, LifecycleEffect, LifecycleError, LifecycleInput, LifecyclePhase, LifecycleState, Peer};
use crate::spec::support::{new_log, take, Log, Recorder};
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
        Self::open_pending(saved_epoch, saved_generation, saved_identity, false, reply)
    }

    fn open_pending(saved_epoch: u64, saved_generation: u64, saved_identity: Option<u64>, saved_unowned_pending: bool, reply: Option<u64>) -> Self {
        let mut det = DetScheduler::new(7);
        let log = new_log();
        let addr = det.add::<Lifecycle>("lifecycle", LifecycleState::new(), Recorder::<Lifecycle>::new(&log));
        let (reference, want) = Ref::open(saved_epoch, saved_generation, saved_identity, saved_unowned_pending, reply);
        let mut twin = Self { det, addr, log, reference, history: Vec::new(), steps: Vec::new() };
        twin.send(LifecycleInput::Open { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, reply });
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
fn life_open_direct_pending() {
    let mut twin = Twin::open_pending(4, 2, None, true, Some(7));
    let effects = twin.feed(Event::Saved(1));
    assert_eq!(effects, vec![RefEffect::PublishCell(twin.published().clone()), RefEffect::EpochStarted(5, None, 2), RefEffect::ReadIntents(5)]);
    assert!(twin.replies().is_empty());
    assert!(twin.feed(Event::RestoreClassified(RefOwner::Session, 4)).is_empty());
    assert!(twin.replies().is_empty());
    assert_eq!(twin.feed(Event::RestoreClassified(RefOwner::Session, 5)), vec![RefEffect::RestoreReady(5)]);
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

#[cfg(test)]
mod lifecycle_ending_tests;
#[cfg(test)]
mod lifecycle_closing_tests;

fn orders(items: &[RefOwner]) -> Vec<Vec<RefOwner>> {
    if items.len() <= 1 { return vec![items.to_vec()]; }
    (0..items.len()).flat_map(|at| {
        let mut rest = items.to_vec();
        let first = rest.remove(at);
        orders(&rest).into_iter().map(move |tail| std::iter::once(first).chain(tail).collect())
    }).collect()
}

#[test]
fn life_restore_classified_every_order() {
    let count = |effects: &[RefEffect], wanted: &RefEffect| effects.iter().filter(|effect| *effect == wanted).count();
    for started in orders(&[RefOwner::Outbox, RefOwner::Transfer]) {
        for classified in orders(&[RefOwner::Outbox, RefOwner::Transfer, RefOwner::Session]) {
            let mut twin = Twin::open(4, 2, Some(9), Some(7));
            twin.feed(Event::Saved(1));
            let read: Vec<Vec<RefEffect>> = started.iter().map(|owner| twin.feed(Event::Started(*owner, 5))).collect();
            assert_eq!(read.iter().map(|effects| count(effects, &RefEffect::ReadIntents(5))).collect::<Vec<_>>(), vec![0, 1], "{started:?}");
            let ready: Vec<Vec<RefEffect>> = classified.iter().map(|owner| twin.feed(Event::RestoreClassified(*owner, 5))).collect();
            assert_eq!(ready.iter().map(|effects| count(effects, &RefEffect::RestoreReady(5))).collect::<Vec<_>>(), vec![0, 0, 1], "{classified:?}");
            assert_eq!(twin.replies(), vec![(7, Ok(()))], "{classified:?}");
        }
    }
}

/// The `Published` of the owner while a models reset waits for the Store (state `Resetting`, identity kept).
pub(crate) fn resetting_published() -> crate::owners::contract::Published {
    let twin = resetting();
    twin.det.state(twin.addr).expect("the owner state").published().clone()
}
