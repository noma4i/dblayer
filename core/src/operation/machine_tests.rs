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
fn the_sql_guard_lists_exactly_the_facts_the_machine_accepts() {
    assert_eq!(OperationEvent::Start.guard_sql(), "((status = 'pending' AND transport_started = 0))");
    assert_eq!(OperationEvent::Postpone.guard_sql(), "((status = 'pending' AND transport_started = 1))");
    assert_eq!(OperationEvent::Retry.guard_sql(), "(status = 'failed' OR status = 'delivery_unknown')");
    assert_eq!(OperationEvent::Fail(Failed).guard_sql(), "(status = 'pending' OR status = 'failed' OR status = 'delivery_unknown')");
    assert_eq!(OperationEvent::Fail(Pending).guard_sql(), "0");
}
