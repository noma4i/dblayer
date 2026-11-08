use super::call_rule::{call_verdict, CallVerdict};
use crate::owners::calls::CallError;
use crate::owners::contract::{Published, PublishedState};

const STATES: [PublishedState; 7] = [PublishedState::NoSession, PublishedState::Session, PublishedState::Ending, PublishedState::EndingFailed, PublishedState::Resetting, PublishedState::StorageFailed, PublishedState::Closing];

/// life_prop_single_published: the verdict of a call is a function of one snapshot, the table "Platform calls" row by row.
#[test]
fn life_prop_single_published() {
    for state in STATES {
        for identity in [None, Some(7)] {
            for epoch in [3, 4] {
                let published = Published { epoch: 3, identity, generation: 1, state, closed: state == PublishedState::Closing };
                let want = match state {
                    PublishedState::Closing => CallVerdict::Closed,
                    PublishedState::StorageFailed => CallVerdict::Refuse(CallError::Storage),
                    PublishedState::Session | PublishedState::Resetting if identity.is_some() && epoch == 3 => CallVerdict::Pass,
                    _ => CallVerdict::Refuse(CallError::NoSession),
                };
                assert_eq!(call_verdict(&published, epoch, true), want, "{state:?} identity {identity:?} call epoch {epoch}");
            }
        }
    }
    let open = Published { closed: false, ..Published::initial() };
    assert_eq!(call_verdict(&open, 0, false), CallVerdict::Pass, "a database without a session client hands every call to the owners");
    assert_eq!(call_verdict(&Published { closed: true, ..open }, 0, false), CallVerdict::Closed);
}

/// lifecycle.md call table: in `Resetting` a call with the current epoch goes to the owner.
#[test]
fn life_call_resetting() {
    let published = crate::spec::lifecycle_owner_tests::resetting_published();
    assert_eq!(published.state, PublishedState::Resetting);
    assert!(published.identity.is_some());
    assert_eq!(call_verdict(&published, published.epoch, true), CallVerdict::Pass);
}

/// lifecycle.md call table: in `Resetting` a call with another epoch gets `NoSession`.
#[test]
fn life_call_resetting_stale() {
    let published = crate::spec::lifecycle_owner_tests::resetting_published();
    assert_eq!(published.state, PublishedState::Resetting);
    assert_eq!(call_verdict(&published, published.epoch + 1, true), CallVerdict::Refuse(CallError::NoSession));
}
