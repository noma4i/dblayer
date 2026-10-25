use OperationStatus::*;

use super::*;

fn facts(status: OperationStatus, transport_started: bool) -> OperationFacts {
    OperationFacts { status, transport_started }
}

#[test]
fn each_event_applies_only_from_its_source_facts() {
    assert_eq!(facts(Pending, false).next(OperationEvent::Start), Some(facts(Pending, true)));
    assert_eq!(facts(Pending, true).next(OperationEvent::Start), None);
    assert_eq!(facts(Pending, true).next(OperationEvent::Postpone), Some(facts(Pending, false)));
    assert_eq!(facts(Pending, false).next(OperationEvent::Postpone), None);
    assert_eq!(facts(Pending, true).next(OperationEvent::Fail(DeliveryUnknown)), Some(facts(DeliveryUnknown, true)));
    assert_eq!(facts(Failed, true).next(OperationEvent::Retry), Some(facts(Pending, false)));
    assert_eq!(facts(Pending, false).next(OperationEvent::Retry), None);
    assert_eq!(facts(DeliveryUnknown, true).next(OperationEvent::Close(RolledBack)), Some(facts(RolledBack, true)));
    assert_eq!(facts(Committed, false).next(OperationEvent::Close(RolledBack)), None);
    assert_eq!(facts(Pending, false).next(OperationEvent::Fail(Pending)), None);
}

#[test]
fn inv_op_04_only_the_transitions_of_the_state_table_apply() {
    let events = [
        OperationEvent::Start,
        OperationEvent::Postpone,
        OperationEvent::Fail(Failed),
        OperationEvent::Fail(DeliveryUnknown),
        OperationEvent::Fail(Pending),
        OperationEvent::Fail(Committed),
        OperationEvent::Fail(RolledBack),
        OperationEvent::Retry,
        OperationEvent::Close(Committed),
        OperationEvent::Close(RolledBack),
        OperationEvent::Close(Pending),
        OperationEvent::Close(Failed),
        OperationEvent::Close(DeliveryUnknown),
    ];
    let mut wrong = Vec::new();
    for (status, started) in [(Pending, false), (Pending, true), (Failed, true), (DeliveryUnknown, true), (Committed, true), (RolledBack, true)] {
        for event in events {
            let source = facts(status, started);
            let accepted = match (status, started, event) {
                (Pending, false, OperationEvent::Start) => Some(facts(Pending, true)),
                (Pending, true, OperationEvent::Postpone) => Some(facts(Pending, false)),
                (Pending, _, OperationEvent::Fail(to @ (Failed | DeliveryUnknown))) => Some(facts(to, started)),
                (Pending, _, OperationEvent::Close(to @ (Committed | RolledBack))) => Some(facts(to, started)),
                (Failed | DeliveryUnknown, true, OperationEvent::Retry) => Some(facts(Pending, false)),
                (Failed | DeliveryUnknown, true, OperationEvent::Close(RolledBack)) => Some(facts(RolledBack, true)),
                _ => None,
            };
            if source.next(event) != accepted {
                wrong.push(format!("{status:?}/{started} x {event:?}: {:?}, expected {accepted:?}", source.next(event)));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
