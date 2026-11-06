use super::*;

fn transfer_pair() -> RefPending {
    let owner = crate::transport::RequestOwner::Transfer { operation_id: 4, item_id: "key-1".into(), step: crate::owners::write::upload::UploadStage::Create };
    stored(Some(owner), 3, 7)
}

fn released_before_open(req_id: u64) -> Twin {
    let mut twin = Twin::new();
    assert!(twin.feed(Input::Release { req_id }).is_empty());
    twin.feed(open(Some((CRED, 6)), Some(transfer_pair()), false, Secret::Present(CRED)));
    assert!(matches!(&twin.reference.state, RefState::PendingHeld { pending } if pending.request_id == 3));
    twin
}

#[test]
fn sess_release_before_restored() {
    let mut twin = released_before_open(3);
    let effects = twin.feed(Input::Restored { epoch: EPOCH, pending: Some(transfer_pair()) });
    assert!(matches!(&effects[..], [RefEffect::RestoreClassified { .. }, RefEffect::HttpRequest { pending, .. }] if pending.request_id == 3 && pending.author.is_none() && pending.owner == Some(crate::transport::RequestOwner::Detached)), "the remembered Release frees the pair: {effects:?}");
    assert!(matches!(twin.reference.state, RefState::Calling { .. }));
    assert!(twin.reference.released.is_empty(), "Restored clears the remembered Releases");

    let mut foreign = released_before_open(99);
    foreign.feed(Input::Restored { epoch: EPOCH, pending: Some(transfer_pair()) });
    assert!(matches!(foreign.reference.state, RefState::PendingHeld { .. }), "a foreign Release leaves the pair held");
    assert!(foreign.reference.released.is_empty());
    assert!(matches!(&foreign.feed(Input::Release { req_id: 3 })[..], [RefEffect::HttpRequest { .. }]), "a Release after Restored frees the pair at once");

    let mut cleared = Twin::new();
    cleared.feed(Input::Release { req_id: 3 });
    cleared.feed(Input::Restored { epoch: EPOCH, pending: None });
    assert!(cleared.feed(Input::Release { req_id: 5 }).is_empty());
    assert!(cleared.reference.released.is_empty(), "a Release in Idle after Restored is not remembered");
    cleared.feed(open(Some((CRED, 6)), Some(transfer_pair()), false, Secret::Present(CRED)));
    assert!(matches!(cleared.reference.state, RefState::PendingHeld { .. }), "a Release cleared at Restored does not outlive it");
}
