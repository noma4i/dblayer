//! The rule of a call of the platform for account data: one `Published` snapshot decides the outcome.

use crate::owners::calls::CallError;
use crate::owners::contract::{Published, PublishedState};
use crate::runtime::Epoch;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum CallVerdict {
    Closed,
    Refuse(CallError),
    Pass,
}

pub(super) fn call_verdict(published: &Published, epoch: Epoch, with_session: bool) -> CallVerdict {
    if !with_session {
        return if published.closed { CallVerdict::Closed } else { CallVerdict::Pass };
    }
    match published.state {
        PublishedState::Closing => CallVerdict::Closed,
        PublishedState::StorageFailed => CallVerdict::Refuse(CallError::Storage),
        PublishedState::Ending | PublishedState::EndingFailed | PublishedState::NoSession => CallVerdict::Refuse(CallError::NoSession),
        PublishedState::Session | PublishedState::Resetting if published.identity.is_none() || published.epoch != epoch => CallVerdict::Refuse(CallError::NoSession),
        PublishedState::Session | PublishedState::Resetting => CallVerdict::Pass,
    }
}
