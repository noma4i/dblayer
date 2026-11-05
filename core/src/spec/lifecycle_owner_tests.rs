//! Owner-level tests of `specs/machines/lifecycle.md` (Lifecycle): the owner on `DetScheduler` and the reference table (`ref_lifecycle.rs`)
//! get the same input, after each one the effects, the replies, the state and `Published` must be equal. Names are the `Test` column.

use std::collections::BTreeSet;

use super::ref_lifecycle::{Answer, Cause as RefCause, Effect as RefEffect, Event, Lifecycle as Ref, Owner as RefOwner, Published as RefPublished, State as RefState};
use crate::owners::contract::{CredentialCommitted, Drained, ServerEnded};
use crate::owners::lifecycle::{Lifecycle, LifecycleEffect, LifecycleError, LifecycleInput, LifecycleState, Peer, LifecyclePhase};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};
use crate::owners::support::{new_log, take, Log, Recorder};

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
    }
}

fn published(published: &crate::owners::contract::Published) -> RefPublished {
    RefPublished { epoch: published.epoch, identity: published.identity, generation: published.generation, closed: published.closed }
}

fn state_of(phase: &LifecyclePhase) -> RefState {
    match phase {
        LifecyclePhase::NoSession { epoch } => RefState::NoSession { epoch: *epoch },
        LifecyclePhase::Session { epoch, identity } => RefState::Session { epoch: *epoch, identity: *identity },
        LifecyclePhase::Ending { epoch_old, cause: c, next, replies, failed } => RefState::Ending { epoch_old: *epoch_old, cause: cause(*c), next: *next, replies: replies.clone(), failed: *failed },
        LifecyclePhase::Resetting { epoch, identity, models, queued_models, replies, pending_credential } => RefState::Resetting { epoch: *epoch, identity: *identity, models: models.clone(), queued: queued_models.clone(), replies: replies.clone(), pending: *pending_credential },
        LifecyclePhase::Closing { waiting, clearing, replies } => RefState::Closing { waiting: waiting.iter().map(|p| ref_owner(*p)).collect(), clearing: *clearing, replies: replies.clone() },
        LifecyclePhase::Closed => RefState::Closed,
    }
}

fn effect_of(effect: &LifecycleEffect) -> Option<RefEffect> {
    Some(match effect {
        LifecycleEffect::Publish(p) => RefEffect::Publish(published(p)),
        LifecycleEffect::EpochStarted { epoch, identity, generation } => RefEffect::EpochStarted(*epoch, *identity, *generation),
        LifecycleEffect::EpochEnded { epoch, cause: c } => RefEffect::EpochEnded(*epoch, cause(*c)),
        LifecycleEffect::ModelsReset { epoch, generation, models } => RefEffect::ModelsReset(*epoch, *generation, models.clone()),
        LifecycleEffect::Closing => RefEffect::Closing,
        LifecycleEffect::ClearAccount { epoch_old, cause: c, generation_new } => RefEffect::ClearAccount(*epoch_old, cause(*c), *generation_new),
        LifecycleEffect::ClearModels { epoch, generation_new, models } => RefEffect::ClearModels(*epoch, *generation_new, models.clone()),
        LifecycleEffect::StoreClose => RefEffect::StoreClose,
        LifecycleEffect::ResetModelsSelf(models) => RefEffect::ResetModelsSelf(models.clone()),
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
    /// Both sides start at `Open` without a session.
    pub fn new() -> Self { Self::open(0, 0, None, &[]) }

    pub fn open(saved_epoch: u64, saved_generation: u64, saved_identity: Option<u64>, changed: &[&str]) -> Self {
        let mut det = DetScheduler::new(7);
        let log = new_log();
        let addr = det.add::<Lifecycle>("lifecycle", LifecycleState::new(), Recorder::<Lifecycle>::new(&log));
        let (reference, want) = Ref::open(saved_epoch, saved_generation, saved_identity, models(changed));
        let mut twin = Self { det, addr, log, reference, history: Vec::new(), steps: Vec::new() };
        twin.send(LifecycleInput::Open { saved_epoch, saved_generation, saved_identity, changed_models: models(changed) });
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
            })),
            _ => None,
        }).collect();
        (raw.iter().filter_map(effect_of).collect(), replies)
    }

    fn same_state(&self) {
        let state = self.det.state(self.addr).expect("the owner state");
        assert_eq!(state_of(state.phase()), self.reference.state, "the same state");
        assert_eq!(published(state.published()), self.reference.published, "the same Published");
    }

    pub fn state(&self) -> &RefState { &self.reference.state }

    pub fn published(&self) -> &RefPublished { &self.reference.published }

    /// Feeds one input of the table to both sides and compares everything. Returns the effects of the reference.
    pub fn feed(&mut self, event: Event) -> Vec<RefEffect> {
        let epoch_old = match &self.reference.state {
            RefState::Ending { epoch_old, .. } => *epoch_old,
            _ => 0,
        };
        let input = match event.clone() {
            Event::Credential(identity) => LifecycleInput::CredentialCommitted(CredentialCommitted { identity }),
            Event::SessionReset(reply) => LifecycleInput::SessionReset { reply },
            Event::ServerEnded(epoch) => LifecycleInput::ServerEnded(ServerEnded { epoch }),
            Event::ResetModels(names, reply) => LifecycleInput::ResetModels { models: names, reply },
            Event::Cleared => LifecycleInput::Cleared { epoch_old },
            Event::ModelsCleared(generation_new) => LifecycleInput::ModelsCleared { generation_new },
            Event::ClearFailed => LifecycleInput::ClearFailed { error: SQL.into() },
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
}

fn credential(identity: u64) -> Event { Event::Credential(identity) }
fn reset(reply: u64) -> Event { Event::SessionReset(reply) }
fn reset_models(names: &[&str], reply: u64) -> Event { Event::ResetModels(models(names), Some(reply)) }
fn close(reply: u64) -> Event { Event::Close(reply) }
fn drain_all() -> Vec<Event> { super::ref_lifecycle::OWNERS.into_iter().map(Event::Drained).collect() }
fn session(identity: u64) -> Twin {
    let mut twin = Twin::new();
    twin.feed(credential(identity));
    twin
}
/// Session 1, `SessionReset` issued, the Store has not answered.
fn ending() -> Twin {
    let mut twin = session(1);
    twin.feed(reset(1));
    twin
}
/// Session 1, `ResetModels` issued, the Store has not answered.
fn resetting() -> Twin {
    let mut twin = session(1);
    twin.feed(reset_models(&["Moment"], 1));
    twin
}
/// NoSession, `ResetModels` issued.
fn resetting_signed_out() -> Twin {
    let mut twin = Twin::new();
    twin.feed(reset_models(&["Moment"], 1));
    twin
}
fn closing_session() -> Twin {
    let mut twin = session(1);
    twin.feed(close(1));
    twin
}

#[test]
fn life_open_no_session() {
    let twin = Twin::open(4, 2, None, &[]);
    assert_eq!(*twin.published(), RefPublished { epoch: 5, identity: None, generation: 2, closed: false });
}

#[test]
fn life_open_session() {
    let twin = Twin::open(4, 2, Some(9), &[]);
    assert_eq!(*twin.published(), RefPublished { epoch: 5, identity: Some(9), generation: 2, closed: false });
}

#[test]
fn life_open_changed_models() {
    let mut twin = Twin::open(4, 2, Some(9), &["Moment"]);
    let effects = twin.feed(Event::ResetModels(models(&["Moment"]), None));
    assert_eq!(effects, vec![RefEffect::ClearModels(5, 3, models(&["Moment"]))], "the self-sent ResetModels clears the changed models");
    assert!(matches!(twin.state(), RefState::Resetting { replies, .. } if replies.is_empty()), "no caller waits for it");
}

#[test]
fn life_nosession_credential_owner() { session(1); }

#[test]
fn life_nosession_reset_owner() { Twin::new().feed(reset(1)); }

#[test]
fn life_nosession_server_ended() {
    let mut twin = Twin::new();
    twin.feed(Event::ServerEnded(1));
    twin.feed(Event::ServerEnded(7));
}

#[test]
fn life_nosession_reset_models() { resetting_signed_out(); }

#[test]
fn life_nosession_stray_store() { Twin::new().feed_all([Event::Cleared, Event::ModelsCleared(1), Event::ClearFailed]); }

#[test]
fn life_nosession_close_owner() { Twin::new().feed(close(1)); }

#[test]
fn life_nosession_drained() { Twin::new().feed(Event::Drained(RefOwner::Feed)); }

#[test]
fn life_session_same_credential_owner() { session(1).feed(credential(1)); }

#[test]
fn life_session_identity_changed() { session(1).feed(credential(2)); }

#[test]
fn life_session_reset_owner() { ending(); }

#[test]
fn life_session_server_ended_owner() { session(1).feed(Event::ServerEnded(2)); }

#[test]
fn life_session_stale_server_ended() { session(1).feed(Event::ServerEnded(1)); }

#[test]
fn life_session_reset_models() { resetting(); }

#[test]
fn life_session_stray_store() { session(1).feed_all([Event::Cleared, Event::ModelsCleared(1), Event::ClearFailed]); }

#[test]
fn life_session_close_owner() { closing_session(); }

#[test]
fn life_session_drained() { session(1).feed(Event::Drained(RefOwner::Calls)); }

#[test]
fn life_ending_credential() { ending().feed(credential(2)); }

#[test]
fn life_ending_credential_after_failure() {
    let mut twin = ending();
    twin.feed_all([Event::ClearFailed, credential(2)]);
}

#[test]
fn life_ending_reset_joins() { ending().feed(reset(2)); }

#[test]
fn life_ending_reset_after_failure() {
    let mut twin = ending();
    twin.feed_all([Event::ClearFailed, reset(2)]);
}

#[test]
fn life_ending_server_ended() { ending().feed(Event::ServerEnded(2)); }

#[test]
fn life_ending_reset_models() { ending().feed(reset_models(&["Moment"], 2)); }

#[test]
fn life_ending_cleared_signed_out() { ending().feed(Event::Cleared); }

#[test]
fn life_ending_cleared_next_identity() {
    let mut twin = ending();
    twin.feed_all([credential(2), Event::Cleared]);
    assert_eq!(*twin.state(), RefState::Session { epoch: 4, identity: 2 });
}

#[test]
fn life_ending_clear_failed() { ending().feed(Event::ClearFailed); }

#[test]
fn life_ending_stray_models() { ending().feed(Event::ModelsCleared(5)); }

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
    twin.feed_all([reset_models(&["Other"], 2), Event::ModelsCleared(1)]);
    assert!(matches!(twin.state(), RefState::Resetting { models: current, .. } if *current == models(&["Other"])));
}

#[test]
fn life_resetting_cleared() { resetting().feed(Event::ModelsCleared(1)); }

#[test]
fn life_resetting_cleared_then_start() {
    let mut twin = resetting_signed_out();
    twin.feed_all([credential(2), Event::ModelsCleared(1)]);
    assert_eq!(*twin.state(), RefState::Session { epoch: 2, identity: 2 });
}

#[test]
fn life_resetting_clear_failed() {
    resetting().feed(Event::ClearFailed);
    resetting_signed_out().feed(Event::ClearFailed);
}

#[test]
fn life_resetting_clear_failed_then_start() {
    let mut twin = resetting_signed_out();
    twin.feed_all([credential(2), Event::ClearFailed]);
    assert_eq!(*twin.published(), RefPublished { epoch: 2, identity: Some(2), generation: 0, closed: false }, "the committed credential is not lost");
}

#[test]
fn life_resetting_stray_cleared() { resetting().feed(Event::Cleared); }

#[test]
fn life_resetting_close() { resetting().feed(close(2)); }

#[test]
fn life_resetting_drained() { resetting().feed(Event::Drained(RefOwner::Feed)); }

#[test]
fn life_closing_last_drained() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    assert_eq!(*twin.state(), RefState::Closed);
}

#[test]
fn life_closing_drained() { closing_session().feed(Event::Drained(RefOwner::Feed)); }

#[test]
fn life_closing_clear_done_last() {
    let mut twin = ending();
    twin.feed(close(2));
    twin.feed_all(drain_all());
    assert!(matches!(twin.state(), RefState::Closing { clearing: true, .. }), "the clearing still runs");
    twin.feed(Event::Cleared);
    assert_eq!(*twin.state(), RefState::Closed);
}

#[test]
fn life_closing_clear_done() {
    let mut twin = ending();
    twin.feed_all([close(2), Event::Cleared]);
    assert!(matches!(twin.state(), RefState::Closing { clearing: false, .. }));
}

#[test]
fn life_closing_close_joins() { closing_session().feed(close(2)); }

#[test]
fn life_closing_refuses() {
    let mut twin = closing_session();
    twin.feed_all([credential(2), reset(2), Event::ServerEnded(1), reset_models(&["Moment"], 3), Event::ResetModels(models(&["Moment"]), None)]);
}

#[test]
fn life_closed_refuses_owner() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    twin.feed_all([credential(2), reset(2), reset_models(&["Moment"], 3), Event::ResetModels(models(&["Moment"]), None), close(4), Event::Cleared]);
}

/// The table "platform calls" is the gate before the owners: it reads `phase` and `epoch` of the owner state.
#[test]
fn life_call_nosession_data_owner() { assert!(matches!(Twin::new().state(), RefState::NoSession { .. })); }

#[test]
fn life_call_current_epoch_owner() { assert!(matches!(session(1).state(), RefState::Session { epoch: 2, .. })); }

#[test]
fn life_call_stale_epoch_owner() {
    let twin = session(1);
    assert!(matches!(twin.state(), RefState::Session { epoch, .. } if *epoch != 0), "a call with another epoch is stale");
}

#[test]
fn life_call_ending() {
    let twin = ending();
    assert_eq!(twin.published().epoch, 2, "Published keeps the old epoch");
    assert!(matches!(twin.state(), RefState::Ending { epoch_old: 2, .. }));
}

#[test]
fn life_call_resetting() { assert!(matches!(resetting().state(), RefState::Resetting { epoch: 2, identity: Some(1), .. })); }

#[test]
fn life_call_resetting_stale() { assert!(matches!(resetting_signed_out().state(), RefState::Resetting { identity: None, .. })); }

#[test]
fn life_call_closed_owner() {
    let twin = closing_session();
    assert!(twin.published().closed);
}

#[test]
fn life_wait_session_reset() {
    let mut twin = ending();
    let effects = twin.feed(Event::Cleared);
    assert!(effects.iter().any(|e| matches!(e, RefEffect::EpochStarted(3, None, 1))));
    let mut failing = ending();
    failing.feed(Event::ClearFailed);
}

#[test]
fn life_wait_close() {
    let mut twin = closing_session();
    twin.feed(close(2));
    twin.feed_all(drain_all());
}

#[test]
fn life_wait_open() {
    let twin = Twin::open(0, 0, None, &[]);
    assert_eq!(twin.published().epoch, 1);
}

// Properties: seeded random traces, the owner against the reference, then the formula over the effects of the owner.

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

fn random_event(seed: &mut u64, step: u64) -> Event {
    let identity = 1 + next(seed) % 2;
    match next(seed) % 12 {
        0 | 1 => Event::Credential(identity),
        2 => Event::SessionReset(step),
        3 => Event::ServerEnded(1 + next(seed) % 6),
        4 => Event::ResetModels(models(&["Moment"]), Some(step)),
        5 | 6 => Event::Cleared,
        7 | 8 => Event::ModelsCleared(1 + next(seed) % 8),
        9 => Event::ClearFailed,
        10 => Event::Close(step),
        _ => Event::Drained(super::ref_lifecycle::OWNERS[(next(seed) % 8) as usize]),
    }
}

/// A trace of `len` random inputs; the Store answers are random too.
fn trace(seed: u64, len: u64) -> Twin {
    let mut seed = seed;
    let mut twin = Twin::new();
    for step in 1..=len {
        let event = random_event(&mut seed, step);
        twin.feed(event);
    }
    twin
}

fn traces() -> impl Iterator<Item = Twin> { (1..=40).map(|seed| trace(seed, 30)) }

#[test]
fn life_prop_epoch_monotonic_owner() {
    for twin in traces() {
        let mut last = 0;
        for effect in &twin.history {
            if let LifecycleEffect::Publish(p) = effect {
                assert!(p.epoch >= last, "Published.epoch {} after {last}", p.epoch);
                last = p.epoch;
            }
        }
    }
}

#[test]
fn life_prop_no_work_after_end_owner() {
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
fn life_prop_clear_before_start_owner() {
    for twin in traces() {
        let mut clearing = false;
        for (event, effects) in &twin.steps {
            for effect in effects {
                match effect {
                    LifecycleEffect::ClearAccount { .. } => clearing = true,
                    LifecycleEffect::EpochStarted { identity: Some(_), .. } => assert!(!clearing || *event == Event::Cleared, "an epoch with an identity starts before the account clearing answered"),
                    _ => {}
                }
            }
            if *event == Event::Cleared {
                clearing = false;
            }
        }
    }
}

/// Every command with a reply is answered once when the Store answers and the owners drain.
#[test]
fn life_prop_reply_once_owner() {
    for seed in 1..=40 {
        let mut twin = trace(seed, 30);
        for _ in 0..4 {
            twin.feed(Event::Cleared);
            twin.feed(Event::ModelsCleared(100 + seed));
            twin.feed(Event::Cleared);
        }
        twin.feed(close(1_000));
        twin.feed_all(drain_all());
        twin.feed_all([Event::Cleared, Event::ModelsCleared(500), Event::ClearFailed]);
        let mut seen: Vec<u64> = Vec::new();
        for effect in &twin.history {
            if let LifecycleEffect::Reply { id, .. } = effect {
                assert!(!seen.contains(id), "reply {id} comes twice");
                seen.push(*id);
            }
        }
        assert!(seen.contains(&1_000), "close is answered");
        assert_eq!(*twin.state(), RefState::Closed);
    }
}

#[test]
fn life_prop_close_drains_owner() {
    let mut twin = session(1);
    twin.feed(close(1));
    let owners = super::ref_lifecycle::OWNERS;
    for (index, owner) in owners.into_iter().enumerate() {
        let effects = twin.feed(Event::Drained(owner));
        let last = index + 1 == owners.len();
        assert_eq!(effects.contains(&RefEffect::StoreClose), last, "close ends with the last Drained and not before");
    }
    assert!(twin.published().closed);
}

#[test]
fn life_prop_generation() {
    for twin in traces() {
        let mut last = 0;
        for effect in &twin.history {
            if let LifecycleEffect::Publish(p) = effect {
                assert!(p.generation >= last, "generation {} after {last}", p.generation);
                last = p.generation;
            }
        }
    }
}

/// `EraseAccount` keeps `encrypted_*` unless the cause is `SessionReset` or `ServerEnded`: the cause goes to the Store as it is.
#[test]
fn life_prop_session_kept_owner() {
    for twin in traces() {
        for effect in &twin.history {
            if let LifecycleEffect::ClearAccount { cause, .. } = effect {
                assert!(matches!(cause, Cause::SessionReset | Cause::ServerEnded | Cause::IdentityChanged), "ClearAccount with the cause {cause:?}");
            }
        }
    }
    let mut twin = session(1);
    let effects = twin.feed(credential(2));
    assert!(effects.contains(&RefEffect::ClearAccount(2, RefCause::IdentityChanged, 1)), "a change of identity clears with IdentityChanged");
}

#[test]
fn life_prop_idle_owner() {
    let mut twin = trace(3, 30);
    twin.send(LifecycleInput::Idle);
    assert!(twin.drain().0.is_empty(), "no input, no effect");
}

#[test]
fn life_prop_credential_kept() {
    for credential_id in [1, 2] {
        let mut twin = resetting_signed_out();
        twin.feed(credential(credential_id));
        twin.feed(Event::ModelsCleared(1));
        assert_eq!(twin.published().identity, Some(credential_id));
    }
    let mut twin = resetting_signed_out();
    twin.feed_all([credential(2), Event::ClearFailed]);
    assert_eq!(twin.published().identity, Some(2));
}

#[test]
fn life_prop_every_epoch_ends() {
    for twin in traces() {
        let mut ended: BTreeSet<u64> = BTreeSet::new();
        for effect in &twin.history {
            match effect {
                LifecycleEffect::EpochEnded { epoch, .. } => { ended.insert(*epoch); }
                LifecycleEffect::EpochStarted { epoch, .. } if *epoch > 1 => assert!(ended.contains(&(epoch - 1)) || (*epoch >= 2 && ended.contains(&(epoch - 2))), "epoch {epoch} starts, neither {} nor {} ended", epoch - 1, epoch - 2),
                _ => {}
            }
        }
    }
}

/// The traces of the findings (`lifecycle.md`, "finding traces"), at the level of the owner.
#[test]
fn life_findings_owner() {
    let mut identity = session(1);
    identity.feed_all([credential(2), Event::Cleared]);
    assert_eq!(*identity.state(), RefState::Session { epoch: 4, identity: 2 }, "a change of identity clears first, then starts");

    let mut models_reset = session(1);
    models_reset.feed_all([reset_models(&["Other"], 1), Event::ModelsCleared(1)]);
    assert_eq!(models_reset.published().generation, 1);

    let mut closing_during_clear = ending();
    closing_during_clear.feed(close(2));
    closing_during_clear.feed_all(drain_all());
    closing_during_clear.feed(Event::Cleared);
    assert_eq!(*closing_during_clear.state(), RefState::Closed);

    let mut double_reset = ending();
    double_reset.feed_all([reset(2), Event::Cleared]);
    assert_eq!(double_reset.history.iter().filter(|e| matches!(e, LifecycleEffect::Reply { result: Ok(()), .. })).count(), 2, "both resets are answered");

    let mut signed_out = resetting_signed_out();
    signed_out.feed_all([credential(2), Event::ClearFailed]);
    assert_eq!(signed_out.published().identity, Some(2));

    let mut opened = Twin::new();
    opened.feed(credential(1));
    assert!(opened.history.iter().any(|e| matches!(e, LifecycleEffect::EpochEnded { cause: Cause::SessionStarted, .. })), "a stream open without a session ends when the account is entered");
}
