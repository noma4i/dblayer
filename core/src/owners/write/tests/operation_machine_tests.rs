use crate::owners::write::operation_status::OperationStatus;
use OperationStatus::*;

use super::operation_machine::*;

fn facts(status: OperationStatus) -> OperationFacts {
    OperationFacts { status }
}

#[test]
fn inv_op_04_only_the_transitions_of_the_state_table_apply() {
    let events = [
        OperationEvent::Attempt,
        OperationEvent::Fail(Failed),
        OperationEvent::Fail(Pending),
        OperationEvent::Fail(Committed),
        OperationEvent::Fail(RolledBack),
        OperationEvent::Retry,
        OperationEvent::Close(Committed),
        OperationEvent::Close(RolledBack),
        OperationEvent::Close(Pending),
        OperationEvent::Close(Failed),
    ];
    let mut wrong = Vec::new();
    for status in [Pending, Sending, Failed, Committed, RolledBack] {
        for event in events {
            let accepted = match (status, event) {
                (Pending | Sending, OperationEvent::Attempt) => Some(facts(Sending)),
                (Pending | Sending, OperationEvent::Fail(Failed)) => Some(facts(Failed)),
                (Pending | Sending, OperationEvent::Close(to @ (Committed | RolledBack))) => Some(facts(to)),
                (Failed, OperationEvent::Retry) => Some(facts(Pending)),
                (Failed, OperationEvent::Close(RolledBack)) => Some(facts(RolledBack)),
                _ => None,
            };
            if facts(status).next(event) != accepted {
                wrong.push(format!("{status:?} x {event:?}: {:?}, expected {accepted:?}", facts(status).next(event)));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
