use super::*;

pub(super) fn detached(request_id: u64, sequence: u64) -> RefPending { stored(Some(crate::transport::RequestOwner::Detached), request_id, sequence) }

pub(super) fn restored_detached(twin: &mut Twin, request_id: u64, sequence: u64) -> Vec<RefEffect> {
    twin.feed(Input::Restored { epoch: EPOCH, pending: Some(detached(request_id, sequence)) })
}

pub(super) fn landing_detached(revoked: bool) -> Twin {
    let mut twin = Twin::new();
    restored_detached(&mut twin, 5, 1);
    twin.feed(Input::RestoreReady { epoch: EPOCH });
    let input = if revoked { Input::Revoked { request_id: 5, attempt_id: twin.aid(), body: "{}".into() } } else { twin.response(1) };
    twin.feed(input);
    assert!(matches!(&twin.reference.state, RefState::Landing { pending, .. } if pending.owner == Some(crate::transport::RequestOwner::Detached) && pending.reply.is_none() && pending.author.is_none()));
    twin
}

#[test]
fn sess_landing_detached_committed() {
    let mut twin = landing_detached(false);
    assert!(twin.feed(committed()).is_empty(), "no author hears the outcome and no Answer is stored");
    assert!(matches!(twin.reference.state, RefState::Idle));
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(1), "the confirmed sequence is kept");
    assert!(twin.replies.is_empty());
    let mut ended = landing_detached(true);
    assert_eq!(ended.feed(committed()), vec![RefEffect::ServerEnded { epoch: EPOCH }], "a revoked answer ends the session and nothing else");
    assert!(ended.replies.is_empty());
}

#[test]
fn sess_open_detached_pending() {
    let mut twin = opened(open(Some((CRED, 0)), Some(detached(5, 1)), false, Secret::Present(CRED)));
    assert!(matches!(twin.reference.state, RefState::Idle) && twin.history.is_empty(), "Session holds the slot for no author and sends nothing before Restored");
    assert!(twin.reference.restored_detached.is_none());
    let effects = restored_detached(&mut twin, 5, 1);
    assert_eq!(effects, vec![RefEffect::RestoreClassified { epoch: EPOCH }]);
    assert_eq!(twin.reference.restored_detached.as_ref().map(|pending| pending.request_id), Some(5));
}

#[test]
fn sess_restored_detached_pending() {
    let mut twin = Twin::new();
    assert_eq!(restored_detached(&mut twin, 5, 1), vec![RefEffect::RestoreClassified { epoch: EPOCH }], "no request before RestoreReady");
    assert!(twin.reference.restored_detached.as_ref().is_some_and(|pending| pending.reply.is_none() && pending.owner == Some(crate::transport::RequestOwner::Detached)));
    assert!(twin.reference.restored_direct.is_none(), "the pending is not a direct call");
}

#[test]
fn sess_restore_ready_detached() {
    let mut twin = Twin::new();
    restored_detached(&mut twin, 5, 1);
    let effects = twin.feed(Input::RestoreReady { epoch: EPOCH });
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == 5 && pending.sequence == 1 && pending.reply.is_none()), "the stored pair goes out: {effects:?}");
    assert!(twin.reference.restored_detached.is_none());
    let input = twin.response(1);
    twin.feed_all([input, committed()]);
    assert!(twin.replies.is_empty(), "an answer without a caller shows nothing");
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(1));
}

#[test]
fn sess_restored_detached_on_idle() {
    let mut busy = persisting();
    busy.feed_all([Input::RestoreReady { epoch: EPOCH }]);
    restored_detached(&mut busy, 5, 2);
    busy.feed(committed());
    let input = busy.response(1);
    busy.feed(input);
    let effects = busy.feed(committed());
    assert!(effects.iter().any(|effect| matches!(effect, RefEffect::HttpRequest { pending, .. } if pending.request_id == 5 && pending.sequence == 2 && pending.reply.is_none())), "{effects:?}");
}

#[test]
fn sess_restored_detached_dropped() {
    let mut twin = Twin::new();
    restored_detached(&mut twin, 5, 1);
    twin.feed(ended(EndCause::Other));
    assert!(twin.reference.restored_detached.is_none());
    let mut closing = Twin::new();
    restored_detached(&mut closing, 5, 1);
    closing.feed(Input::Closing);
    assert!(closing.reference.restored_detached.is_none());
}

#[test]
fn sess_prop_detached_crash() {
    let mut twin = Twin::new();
    restored_detached(&mut twin, 5, 1);
    twin.feed(Input::RestoreReady { epoch: EPOCH });
    assert!(twin.feed(call(2)).is_empty(), "the call waits behind the stored pair");
    let input = twin.response(1);
    twin.feed_all([input, committed()]);
    let pairs: Vec<(u64, u64)> = twin.history.iter().filter_map(|effect| match effect { RefEffect::HttpRequest { pending, .. } => Some((pending.request_id, pending.sequence)), _ => None }).collect();
    assert_eq!(pairs, vec![(5, 1)], "only the stored pair is sent again, no new pair was written before it landed");
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(1), "the sequence moves after the confirmed landing");
    assert!(twin.history.iter().any(|effect| matches!(effect, RefEffect::SealProbe { pending, .. } if pending.sequence == 2)), "the queue is freed with the next sequence");
    assert!(twin.history.iter().all(|effect| !matches!(effect, RefEffect::Answer { .. })) && twin.replies.is_empty(), "no author gets the outcome");
}
