use super::*;

pub(super) fn rejected_landing(sequence: u64) -> Twin {
    let mut twin = calling_mutate();
    let input = twin.response(sequence);
    twin.feed(input);
    twin
}

pub(super) fn owned_mutate(author: u64, owner: crate::transport::RequestOwner, req_id: u64) -> Input { Input::Mutate { author, owner: Some(owner), req_id, epoch: EPOCH, path: OTP.into(), body: "{}".into(), purpose: 0 } }

pub(super) fn outbox_owner() -> crate::transport::RequestOwner { crate::transport::RequestOwner::Operation { seq: 1 } }

pub(super) fn transfer_init_owner() -> crate::transport::RequestOwner { crate::transport::RequestOwner::Transfer { operation_id: 1, item_id: "item".into(), step: crate::owners::write::upload::UploadStage::Init } }

fn detached_owner() -> Option<crate::transport::RequestOwner> { Some(crate::transport::RequestOwner::Detached) }

pub(super) fn gate_closed(twin: &Twin, delta: u64) -> Input { Input::GateClosed { request_id: twin.rid() + delta, attempt_id: twin.aid() } }

pub(super) fn new_epoch() -> Input { Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::Session } }

#[test]
fn sess_held_release_stray() {
    let mut twin = held();
    let before = twin.reference.state.clone();
    assert!(twin.feed(Input::Release { req_id: twin.rid() + 1 }).is_empty());
    assert_eq!(twin.reference.state, before, "the held pair keeps its author");
}

#[test]
fn sess_blocked_release() {
    let mut twin = blocked_invalid();
    let RefState::Blocked { pending: Some(before), reason } = twin.reference.state.clone() else { panic!("blocked") };
    assert_eq!(twin.feed(Input::Release { req_id: before.request_id }), vec![answer(7, 11, Outcome::Dropped)], "the removed author hears only Dropped");
    assert_eq!(twin.reference.state, RefState::Blocked { pending: Some(RefPending { author: None, owner: detached_owner(), ..before }), reason }, "the reason and the pair stay");
    assert!(twin.feed(Input::Tick).is_empty(), "no repeat of the write");
    for (author, owner) in [(crate::spec::ref_session::OUTBOX_AUTHOR, outbox_owner()), (crate::spec::ref_session::TRANSFER_AUTHOR, transfer_init_owner())] {
        let mut kept = Twin::new();
        kept.feed_all([owned_mutate(author, owner, 11), committed()]);
        let input = kept.response(1);
        kept.feed_all([input, rejected(Reason::Invalid)]);
        assert_eq!(kept.feed(Input::Release { req_id: 11 }), vec![answer(author, 11, Outcome::Dropped)], "author {author}: Dropped is the answer to Release");
        assert!(matches!(&kept.reference.state, RefState::Blocked { pending: Some(pending), reason: Block::Invalid } if pending.author == Some(author) && pending.owner == detached_owner() && pending.reply.is_none()), "author {author} stays for sess_landing_detached_committed: {:?}", kept.reference.state);
    }
}

#[test]
fn sess_blocked_release_stray() {
    let mut twin = blocked_invalid();
    let before = twin.reference.state.clone();
    assert!(twin.feed(Input::Release { req_id: 99 }).is_empty());
    assert_eq!(twin.reference.state, before);
    let mut lost = opened(open(Some((CRED, 0)), None, true, Secret::Present(CRED)));
    let before = lost.reference.state.clone();
    assert!(lost.feed(Input::Release { req_id: 11 }).is_empty());
    assert_eq!(lost.reference.state, before, "no pending, nothing to release");
}

#[test]
fn sess_landing_release() {
    let mut twin = rejected_landing(1);
    let RefState::Landing { pending, batch, answer: stored, identity, sequence, server_ended } = twin.reference.state.clone() else { panic!("landing") };
    assert_eq!(twin.feed(Input::Release { req_id: pending.request_id }), vec![answer(7, 11, Outcome::Dropped)], "the removed author hears only Dropped");
    assert_eq!(twin.reference.state, RefState::Landing { pending: RefPending { author: None, owner: detached_owner(), ..pending }, batch, answer: stored, identity, sequence, server_ended }, "the stored result and the batch stay");
    assert!(twin.feed(committed()).is_empty(), "the outcome of the server does not reach the removed author");
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(1));
}

#[test]
fn sess_landing_release_stray() {
    let mut twin = rejected_landing(1);
    let before = twin.reference.state.clone();
    assert!(twin.feed(Input::Release { req_id: 99 }).is_empty());
    assert_eq!(twin.reference.state, before);
    assert_eq!(twin.feed(committed()), vec![answer(7, 11, Outcome::Response("{}".into(), false))], "the author of the pair still hears the outcome");
}

#[test]
fn sess_blocked_released_mutate() {
    let mut invalid = blocked_invalid();
    invalid.feed(Input::Release { req_id: 11 });
    let before = invalid.reference.state.clone();
    assert_eq!(invalid.feed(mutate(9, 11)), vec![answer(9, 11, retained(Fail::Invalid))], "the removed author is not bound again");
    assert_eq!(invalid.reference.state, before);
    let mut conflict = rejected_landing(5);
    assert!(matches!(conflict.reference.state, RefState::Blocked { reason: Block::SequenceConflict, .. }));
    conflict.feed(Input::Release { req_id: 11 });
    assert_eq!(conflict.feed(mutate(9, 11)), vec![answer(9, 11, retained(Fail::SequenceConflict))]);
}

#[test]
fn sess_calling_gate_closed() {
    let mut twin = calling();
    let input = gate_closed(&twin, 0);
    assert!(twin.feed(input).is_empty(), "the request did not leave: nothing is answered");
    assert!(twin.in_phase(CallPhase::WaitingNetwork));
    let effects = twin.feed(Input::Online(true));
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == 1 && pending.sequence == 1), "the same pair goes out when the network is back: {effects:?}");
}

#[test]
fn sess_gate_closed_stray() {
    let mut twin = calling();
    for input in [gate_closed(&twin, 9), Input::GateClosed { request_id: twin.rid(), attempt_id: twin.aid() + 9 }] {
        assert!(twin.feed(input).is_empty());
        assert!(matches!(twin.reference.state, RefState::Calling { phase: CallPhase::InFlight(_), .. }), "a request of another attempt does not stop this one");
    }
    let mut idle = Twin::new();
    assert!(idle.feed(Input::GateClosed { request_id: 1, attempt_id: 1 }).is_empty());
    assert!(matches!(idle.reference.state, RefState::Idle));
    let mut persisting = persisting();
    assert!(persisting.feed(Input::GateClosed { request_id: 1, attempt_id: 1 }).is_empty());
    assert!(matches!(persisting.reference.state, RefState::Persisting { .. }));
}

#[test]
fn sess_published_snapshot_request() {
    let mut twin = Twin::new();
    assert_eq!(twin.feed(new_epoch()), vec![RefEffect::SnapshotRequest { epoch: EPOCH + 1 }]);
    assert_eq!(twin.reference.online, None, "the network is unknown until the snapshot");
    twin.feed(call(1));
    assert!(twin.feed(committed()).is_empty(), "no request without a network value");
    let effects = twin.feed(Input::Snapshot { epoch: EPOCH + 1, online: true });
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { .. }]), "the snapshot sends the waiting request: {effects:?}");
    let mut same = Twin::new();
    assert!(same.feed(Input::Published { epoch: EPOCH, identity: None, state: PubState::Session }).is_empty());
    assert_eq!(same.reference.online, Some(true));
    let mut ending = Twin::new();
    assert!(ending.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::Ending }).is_empty());
    assert_eq!(ending.reference.online, Some(true), "only an active publication of a new epoch resets the network value");
}

#[test]
fn sess_snapshot_stale() {
    let mut twin = Twin::new();
    twin.feed(new_epoch());
    for epoch in [EPOCH, EPOCH + 2] {
        assert!(twin.feed(Input::Snapshot { epoch, online: true }).is_empty());
        assert_eq!(twin.reference.online, None, "a snapshot of another epoch is not the answer to this request");
    }
    twin.feed(Input::Snapshot { epoch: EPOCH + 1, online: false });
    assert_eq!(twin.reference.online, Some(false));
}

#[test]
fn sess_online_before_snapshot() {
    let mut twin = Twin::new();
    twin.feed(new_epoch());
    assert!(twin.feed(Input::Online(true)).is_empty());
    assert!(twin.feed(Input::Online(false)).is_empty());
    assert_eq!(twin.reference.online, None, "the snapshot already holds the earlier changes");
    twin.feed(Input::Snapshot { epoch: EPOCH + 1, online: true });
    twin.feed(Input::Online(false));
    assert_eq!(twin.reference.online, Some(false), "after the snapshot every change counts");
}
