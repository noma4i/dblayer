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
