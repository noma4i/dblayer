//! Owner-level tests of the `lifecycle` machine (Lifecycle): the owner on `DetScheduler` gets inputs, the tests read its effects, replies, `Published` and the phase.

use crate::owners::contract::{CredentialCommitted, Drained, Published, PublishedState, ServerEnded};
use crate::owners::lifecycle::{Lifecycle, LifecycleEffect, LifecycleError, LifecycleInput, LifecyclePhase, LifecycleState, Peer, ALL_OWNERS};
use crate::spec::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};

pub(super) const SQL: &str = "disk";

/// The owner alone: every input goes through the scheduler, every effect is kept.
pub(super) struct Twin {
    det: DetScheduler,
    addr: Address<Lifecycle>,
    log: Log<LifecycleEffect>,
    /// Every effect of the owner since the start.
    pub history: Vec<LifecycleEffect>,
    /// Each input with the effects it gave.
    pub steps: Vec<(LifecycleInput, Vec<LifecycleEffect>)>,
}

impl Twin {
    /// Open without a session, the first publication saved.
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
        let mut twin = Self { det, addr, log, history: Vec::new(), steps: Vec::new() };
        twin.feed(LifecycleInput::Open { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, reply });
        twin
    }

    /// Feeds one input, returns its effects without the replies (`replies()` has them).
    pub fn feed(&mut self, input: LifecycleInput) -> Vec<LifecycleEffect> {
        self.det.post(self.addr, EpochStamp::Control, input.clone());
        self.det.run_until_quiet(1_000).expect("the owner is quiet");
        let raw = take(&self.log);
        self.history.extend(raw.iter().cloned());
        self.steps.push((input, raw.clone()));
        raw.into_iter().filter(|effect| !matches!(effect, LifecycleEffect::Reply { .. })).collect()
    }

    pub fn feed_all(&mut self, inputs: impl IntoIterator<Item = LifecycleInput>) { inputs.into_iter().for_each(|input| { self.feed(input); }); }

    fn state(&self) -> &LifecycleState { self.det.state(self.addr).expect("the owner state") }

    pub fn phase(&self) -> &LifecyclePhase { self.state().phase() }

    pub fn published(&self) -> &Published { self.state().published() }

    pub fn cell(&self) -> &Published { self.state().cell() }

    pub fn publications(&self) -> Vec<u64> { self.state().publishing().iter().map(|publication| publication.id).collect() }

    /// The ids of the erases in the Store without an answer, oldest first.
    pub fn erases(&self) -> Vec<u64> {
        match self.phase() {
            LifecyclePhase::Ending { erases, .. } | LifecyclePhase::Closing { erases, .. } => erases.iter().map(|erase| erase.id).collect(),
            _ => Vec::new(),
        }
    }

    /// The id of the oldest erase of the Store.
    pub fn erase(&self) -> u64 { *self.erases().first().expect("an erase is open") }

    /// The Store saves every publication, then Outbox and Transfer start and the three receivers classify the restored intents.
    pub fn settle(&mut self) {
        for _ in 0..6 {
            self.save_all();
            let Some(restoring) = self.state().restoring().cloned() else { break };
            if !restoring.start_waiting.is_empty() {
                restoring.start_waiting.iter().for_each(|owner| { self.feed(started(*owner, restoring.epoch)); });
            } else {
                restoring.restore_waiting.iter().for_each(|owner| { self.feed(classified(*owner, restoring.epoch)); });
            }
        }
    }

    /// The Store answers `Saved` for every open publication, oldest first.
    pub fn save_all(&mut self) {
        for id in self.publications() {
            self.feed(saved(id));
        }
    }

    /// The answers of the owner to its callers, without the open of `new()`.
    pub fn replies(&self) -> Vec<(u64, Result<(), LifecycleError>)> {
        self.history.iter().filter_map(|effect| match effect { LifecycleEffect::Reply { id, result } if *id != 1_000 => Some((*id, *result)), _ => None }).collect()
    }
}

pub(super) fn credential(identity: u64) -> LifecycleInput { LifecycleInput::CredentialCommitted(CredentialCommitted { identity }) }
pub(super) fn reset(reply: u64) -> LifecycleInput { LifecycleInput::SessionReset { reply: Some(reply) } }
pub(super) fn close(reply: u64) -> LifecycleInput { LifecycleInput::Close { reply } }
pub(super) fn server_ended(epoch: u64) -> LifecycleInput { LifecycleInput::ServerEnded(ServerEnded { epoch }) }
pub(super) fn saved(publication_id: u64) -> LifecycleInput { LifecycleInput::Saved { publication_id } }
pub(super) fn save_failed(publication_id: u64) -> LifecycleInput { LifecycleInput::SaveFailed { publication_id, error: SQL.into() } }
pub(super) fn started(owner: Peer, epoch: u64) -> LifecycleInput { LifecycleInput::Started { owner, epoch } }
pub(super) fn classified(owner: Peer, epoch: u64) -> LifecycleInput { LifecycleInput::RestoreClassified { owner, epoch } }
pub(super) fn drained(owner: Peer) -> LifecycleInput { LifecycleInput::Drained(Drained { owner: owner.name() }) }
pub(super) fn drain_all() -> Vec<LifecycleInput> { ALL_OWNERS.into_iter().map(drained).collect() }
pub(super) fn cleared(twin: &Twin) -> LifecycleInput { LifecycleInput::Cleared { erase_id: twin.erase() } }
pub(super) fn clear_failed(twin: &Twin) -> LifecycleInput { LifecycleInput::ClearFailed { erase_id: twin.erase(), error: SQL.into() } }
/// The answers of the Store to ids it never got.
pub(super) fn stray_store_answers() -> [LifecycleInput; 4] { [LifecycleInput::Cleared { erase_id: 999 }, LifecycleInput::ClearFailed { erase_id: 999, error: SQL.into() }, saved(999), save_failed(999)] }

/// Session 1 with the start saved.
pub(super) fn session(identity: u64) -> Twin {
    let mut twin = Twin::new();
    twin.feed(credential(identity));
    twin.settle();
    twin
}
/// Session 1, `SessionReset` issued, the Store has not answered.
pub(super) fn ending() -> Twin {
    let mut twin = session(1);
    twin.feed(reset(1));
    twin.settle();
    twin
}
pub(super) fn closing_session() -> Twin {
    let mut twin = session(1);
    twin.feed(close(1));
    twin.settle();
    twin
}

fn published_of(epoch: u64, identity: Option<u64>, generation: u64, state: PublishedState) -> Published { Published { epoch, identity, generation, state, closed: false } }

#[test]
fn life_open_no_session() {
    let mut twin = Twin::open(4, 2, None, Some(7));
    assert_eq!(*twin.published(), published_of(5, None, 2, PublishedState::NoSession));
    assert!(twin.replies().is_empty(), "open is answered after Saved");
    let published = twin.published().clone();
    let effects = twin.feed(saved(1));
    assert_eq!(effects, vec![LifecycleEffect::PublishCell(published), LifecycleEffect::EpochStarted { epoch: 5, identity: None, generation: 2 }]);
    assert_eq!(twin.replies(), vec![(7, Ok(()))]);
}

#[test]
fn life_open_direct_pending() {
    let mut twin = Twin::open_pending(4, 2, None, true, Some(7));
    let published = twin.published().clone();
    let effects = twin.feed(saved(1));
    assert_eq!(effects, vec![LifecycleEffect::PublishCell(published), LifecycleEffect::EpochStarted { epoch: 5, identity: None, generation: 2 }, LifecycleEffect::ReadIntents { epoch: 5 }]);
    assert!(twin.replies().is_empty());
    assert!(twin.feed(classified(Peer::Session, 4)).is_empty(), "a classification of an older epoch is ignored");
    assert!(twin.replies().is_empty());
    assert_eq!(twin.feed(classified(Peer::Session, 5)), vec![LifecycleEffect::RestoreReady { epoch: 5 }]);
    assert_eq!(twin.replies(), vec![(7, Ok(()))]);
}

#[test]
fn life_open_session() {
    let mut twin = Twin::open(4, 2, Some(9), Some(7));
    assert_eq!(*twin.published(), published_of(5, Some(9), 2, PublishedState::Session));
    assert_eq!(*twin.cell(), published_of(0, None, 0, PublishedState::NoSession), "an active publication is visible after Saved");
    let published = twin.published().clone();
    let effects = twin.feed(saved(1));
    assert_eq!(effects, vec![LifecycleEffect::PublishCell(published), LifecycleEffect::EpochStarted { epoch: 5, identity: Some(9), generation: 2 }]);
    assert!(twin.replies().is_empty(), "open waits for the receivers of the snapshot");
    twin.feed(started(Peer::Outbox, 5));
    assert_eq!(twin.feed(started(Peer::Transfer, 5)), vec![LifecycleEffect::ReadIntents { epoch: 5 }], "the intents are read after Saved and Started of both receivers");
    twin.feed(classified(Peer::Outbox, 5));
    twin.feed(classified(Peer::Transfer, 5));
    assert!(twin.replies().is_empty());
    assert_eq!(twin.feed(classified(Peer::Session, 5)), vec![LifecycleEffect::RestoreReady { epoch: 5 }]);
    assert_eq!(twin.replies(), vec![(7, Ok(()))]);
}

fn orders(items: &[Peer]) -> Vec<Vec<Peer>> {
    if items.len() <= 1 { return vec![items.to_vec()]; }
    (0..items.len()).flat_map(|at| {
        let mut rest = items.to_vec();
        let first = rest.remove(at);
        orders(&rest).into_iter().map(move |tail| std::iter::once(first).chain(tail).collect())
    }).collect()
}

/// `open` is answered exactly when the last receiver has classified, in every delivery order and with no clock.
#[test]
fn life_restore_classified_every_order() {
    let count = |effects: &[LifecycleEffect], wanted: &LifecycleEffect| effects.iter().filter(|effect| *effect == wanted).count();
    for started_order in orders(&[Peer::Outbox, Peer::Transfer]) {
        for classified_order in orders(&[Peer::Outbox, Peer::Transfer, Peer::Session]) {
            let mut twin = Twin::open(4, 2, Some(9), Some(7));
            twin.feed(saved(1));
            let read: Vec<Vec<LifecycleEffect>> = started_order.iter().map(|owner| twin.feed(started(*owner, 5))).collect();
            assert_eq!(read.iter().map(|effects| count(effects, &LifecycleEffect::ReadIntents { epoch: 5 })).collect::<Vec<_>>(), vec![0, 1], "{started_order:?}");
            let ready: Vec<Vec<LifecycleEffect>> = classified_order.iter().map(|owner| twin.feed(classified(*owner, 5))).collect();
            assert_eq!(ready.iter().map(|effects| count(effects, &LifecycleEffect::RestoreReady { epoch: 5 })).collect::<Vec<_>>(), vec![0, 0, 1], "{classified_order:?}");
            assert_eq!(twin.replies(), vec![(7, Ok(()))], "{classified_order:?}");
        }
    }
}

/// A failed save answers `open` with `Storage` once and starts no work of the epoch, whatever else arrives later.
#[test]
fn life_save_failed_answers_open_once() {
    for saved_identity in [None, Some(9)] {
        let mut twin = Twin::open(0, 0, saved_identity, Some(7));
        twin.feed(save_failed(1));
        assert_eq!(twin.cell().state, PublishedState::StorageFailed, "{saved_identity:?}");
        twin.feed_all([started(Peer::Outbox, 1), started(Peer::Transfer, 1), classified(Peer::Session, 1)]);
        assert_eq!(twin.replies(), vec![(7, Err(LifecycleError::Storage))], "{saved_identity:?}: open answers Err(Storage) once, a late restore answer does not answer it again");
        assert!(!twin.history.iter().any(|effect| matches!(effect, LifecycleEffect::EpochStarted { .. } | LifecycleEffect::ReadIntents { .. })), "{saved_identity:?}: no work of the epoch starts");
    }
    let mut twin = Twin::new();
    twin.feed(credential(1));
    let publication = twin.publications()[0];
    twin.feed(save_failed(publication));
    assert_eq!(twin.cell().state, PublishedState::StorageFailed, "the publication of a credential fails the same way");
}

/// The later publication stays when the earlier one fails.
#[test]
fn life_save_failed_stale() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(credential(1));
    twin.feed(saved(2));
    let before = twin.cell().clone();
    twin.feed(save_failed(1));
    assert_eq!(*twin.cell(), before, "the later publication stays");
    assert!(twin.replies().contains(&(7, Err(LifecycleError::Storage))));
}

/// life_storage_failed_retried_on_event: `Online(true)` and `Foreground` write the last active publication again, once per event; `Saved` leaves `StorageFailed`.
#[test]
fn life_storage_failed_retried_on_event() {
    let publishes = |effects: &[LifecycleEffect]| effects.iter().filter(|effect| matches!(effect, LifecycleEffect::Publish { .. })).count();
    let mut twin = Twin::new();
    twin.feed(credential(1));
    let first = twin.publications()[0];
    twin.feed(save_failed(first));
    assert_eq!(twin.cell().state, PublishedState::StorageFailed);
    assert_eq!(publishes(&twin.feed(LifecycleInput::Online(false))), 0, "offline repeats nothing");
    let effects = twin.feed(LifecycleInput::Online(true));
    let retry = twin.publications();
    assert_eq!((publishes(&effects), retry.len(), twin.cell().state), (1, 1, PublishedState::StorageFailed), "one write per event, the state stays until `Saved`");
    twin.feed(save_failed(retry[0]));
    assert_eq!(twin.cell().state, PublishedState::StorageFailed, "a refused retry stays");
    assert_eq!(publishes(&twin.feed(LifecycleInput::Foreground)), 1, "the next event writes again");
    let retry = twin.publications();
    let effects = twin.feed(saved(retry[0]));
    assert_eq!((twin.cell().state, twin.cell().identity), (PublishedState::Session, Some(1)));
    assert!(effects.iter().any(|effect| matches!(effect, LifecycleEffect::EpochStarted { identity: Some(1), .. })), "the epoch of the refused publication starts with the retry: {effects:?}");
    assert_eq!(publishes(&twin.feed(LifecycleInput::Online(true))), 0, "no event writes once the state is left");
}

/// life_network_event_ignored: with no `StorageFailed` an event writes nothing, in every phase.
#[test]
fn life_network_event_ignored() {
    let mut twin = Twin::new();
    let mut ending = ending();
    for fresh in [&mut twin, &mut ending] {
        for input in [LifecycleInput::Online(true), LifecycleInput::Online(false), LifecycleInput::Foreground] {
            assert!(fresh.feed(input).is_empty());
        }
    }
    let mut session = session(1);
    assert!(session.feed(LifecycleInput::Foreground).is_empty());
}

/// The publication of an epoch that ended before its `Saved` answers `Interrupted` once and starts nothing.
#[test]
fn life_saved_stale() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(credential(1));
    twin.save_all();
    assert_eq!(twin.replies().iter().filter(|(id, _)| *id == 7).cloned().collect::<Vec<_>>(), vec![(7, Err(LifecycleError::Interrupted))]);
    assert!(!twin.history.iter().any(|effect| matches!(effect, LifecycleEffect::EpochStarted { epoch: 1, .. })), "EpochStarted of an ended epoch does not go out");
}

/// `Saved` of one publication leaves the other open publications alone.
#[test]
fn life_saved_of_one_publication_leaves_the_others() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(credential(1));
    let second = twin.publications()[1];
    twin.feed(saved(second));
    assert!(!twin.replies().iter().any(|(id, _)| *id == 7), "the later Saved does not answer the earlier publication");
    assert_eq!(twin.publications().len(), 1);
}

/// Answers of the Store to ids it never got change nothing in any phase.
#[test]
fn life_stray_store_answers_are_ignored() {
    let phases: Vec<(&str, fn() -> Twin)> = vec![("signed out", Twin::new), ("session", || session(1)), ("ending", ending), ("closing", closing_session)];
    for (name, make) in phases {
        let mut twin = make();
        for input in stray_store_answers() {
            assert!(twin.feed(input.clone()).is_empty(), "{name}: {input:?}");
        }
    }
}

/// `Drained` outside a close changes nothing.
#[test]
fn life_drained_outside_close_is_ignored() {
    let phases: Vec<(&str, fn() -> Twin)> = vec![("signed out", Twin::new), ("session", || session(1)), ("ending", ending)];
    for (name, make) in phases {
        assert!(make().feed(drained(Peer::Feed)).is_empty(), "{name}");
    }
}

mod lifecycle_ending_tests;
mod lifecycle_closing_tests;
