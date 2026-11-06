fn rejected_landing(sequence: u64) -> Twin {
    let mut twin = calling_mutate();
    let input = twin.response(sequence);
    twin.feed(input);
    twin
}

fn blocked_with_author() -> Twin {
    let mut twin = rejected_landing(1);
    twin.feed(rejected(Reason::Storage));
    assert!(matches!(&twin.reference.state, RefState::Blocked { pending: Some(pending), reason: Block::Storage, landing: Some(_) } if pending.author == Some(7)));
    twin
}

fn gate_closed(twin: &Twin, delta: u64) -> Input { Input::GateClosed { request_id: twin.rid() + delta, attempt_id: twin.aid() } }

fn new_epoch() -> Input { Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::Session } }

#[test]
fn sess_held_release_stray() {
    let mut twin = held();
    let before = twin.reference.state.clone();
    assert!(twin.feed(Input::Release { req_id: twin.rid() + 1 }).is_empty());
    assert_eq!(twin.reference.state, before, "the held pair keeps its author");
}

#[test]
fn sess_blocked_release() {
    let mut twin = blocked_with_author();
    let RefState::Blocked { pending: Some(before), reason, landing } = twin.reference.state.clone() else { panic!("blocked") };
    assert!(twin.feed(Input::Release { req_id: before.request_id }).is_empty(), "the removed author hears nothing");
    assert_eq!(twin.reference.state, RefState::Blocked { pending: Some(RefPending { author: None, ..before }), reason, landing: landing.clone() }, "the reason, the pair and the landing stay");
    let effects = twin.feed(Input::Tick);
    assert!(matches!(&effects[..], [RefEffect::CommitLanding { answer: None, sequence: Some(1), .. }]), "the landing commits with no Answer of the removed author: {effects:?}");
}

#[test]
fn sess_blocked_release_stray() {
    let mut twin = blocked_with_author();
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
    let RefState::Landing { pending, batch, answer, identity, sequence, server_ended } = twin.reference.state.clone() else { panic!("landing") };
    assert!(twin.feed(Input::Release { req_id: pending.request_id }).is_empty());
    assert_eq!(twin.reference.state, RefState::Landing { pending: RefPending { author: None, ..pending }, batch, answer, identity, sequence, server_ended }, "the stored result and the batch stay");
    assert!(twin.feed(committed()).is_empty(), "the removed author hears nothing");
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(1));
}

#[test]
fn sess_landing_release_stray() {
    let mut twin = rejected_landing(1);
    let before = twin.reference.state.clone();
    assert!(twin.feed(Input::Release { req_id: 99 }).is_empty());
    assert_eq!(twin.reference.state, before);
    assert_eq!(twin.feed(committed()), vec![answer(7, 11, Outcome::Response("{}".into()))], "the author of the pair still hears the outcome");
}

#[test]
fn sess_blocked_released_mutate() {
    let mut storage = blocked_with_author();
    storage.feed(Input::Release { req_id: 11 });
    let before = storage.reference.state.clone();
    assert_eq!(storage.feed(mutate(9, 11)), vec![answer(9, 11, permanent(Fail::Storage))], "the removed author is not bound again");
    assert_eq!(storage.reference.state, before);
    let mut conflict = rejected_landing(5);
    assert!(matches!(conflict.reference.state, RefState::Blocked { reason: Block::SequenceConflict, .. }));
    conflict.feed(Input::Release { req_id: 11 });
    assert_eq!(conflict.feed(mutate(9, 11)), vec![answer(9, 11, permanent(Fail::SequenceConflict))]);
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
