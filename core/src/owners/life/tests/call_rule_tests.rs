use super::call_rule::{call_verdict, CallVerdict};
use crate::owners::calls::CallError;
use crate::owners::contract::{Published, PublishedState};

const STATES: [PublishedState; 6] = [PublishedState::NoSession, PublishedState::Session, PublishedState::Ending, PublishedState::EndingFailed, PublishedState::StorageFailed, PublishedState::Closing];

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
                    PublishedState::Session if identity.is_some() && epoch == 3 => CallVerdict::Pass,
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
