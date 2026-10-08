use super::*;
use crate::ddl::migrate_core;
use rusqlite::Connection;

fn database() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    migrate_core(&connection).unwrap();
    connection
}

fn ticket(seq: i64, reason: QuarantineReason) -> QuarantineTicket {
    QuarantineTicket { seq, reason }
}

#[test]
fn eviction_drops_oldest_plan_rejections_over_the_cap() {
    use QuarantineReason::*;
    let rejected = [ticket(3, PlanRowRejected), ticket(1, PlanRowRejected), ticket(2, PlanRowRejected)];
    let five: Vec<_> = (1..=5).map(|s| ticket(s, PlanRowRejected)).collect();
    let orphans: Vec<_> = (1..=4).map(|s| ticket(s, OrphanTempRow)).collect();
    let mixed = [ticket(1, OrphanTempRow), ticket(2, OrphanTempRow), ticket(3, PlanRowRejected), ticket(4, PlanRowRejected), ticket(5, PlanRowRejected)];
    assert!(is_evictable(PlanRowRejected));
    assert!(!is_evictable(OrphanTempRow));
    assert!(!is_evictable(DecodeFailed));
    assert!(evicted(&rejected, 3).is_empty());
    assert_eq!(evicted(&rejected, 2), [1]);
    assert_eq!(evicted(&five, 2), [1, 2, 3]);
    assert!(evicted(&orphans, 1).is_empty());
    assert_eq!(evicted(&mixed, 2), [3]);
}

#[test]
fn restore_order_follows_sequence() {
    use QuarantineReason::*;
    assert_eq!(restore_order(&[ticket(3, DecodeFailed), ticket(1, OrphanTempRow), ticket(2, PlanRowRejected)]), [1, 2, 3]);
}

#[test]
fn reasons_decide_open_operations_and_rejected_record_count() {
    for reason in QuarantineReason::ALL {
        assert_eq!(keeps_operation_open(reason), reason == QuarantineReason::ReplaceNormalizeRejected);
        assert_eq!(counts_as_rejected_record(reason), matches!(reason, QuarantineReason::DecodeFailed | QuarantineReason::IdentityMismatch));
    }
    assert_eq!(QuarantineReason::PlanRowRejected.as_str(), "plan-row-rejected");
    for reason in QuarantineReason::ALL {
        assert_eq!(QuarantineReason::parse(reason.as_str()), Some(reason));
    }
}

#[test]
fn add_returns_sequential_ids_and_counts_by_reason() {
    let connection = database();
    let first = quarantine_add(&connection, "moment", b"{}", QuarantineReason::DecodeFailed).unwrap();
    let second = quarantine_add(&connection, "moment", b"{}", QuarantineReason::DecodeFailed).unwrap();
    assert_eq!((first, second), (1, 2));
    assert_eq!(quarantine_counts(&connection).unwrap()["decode-failed"], 2);
}

#[test]
fn oldest_respects_limit_and_filters_by_reason() {
    let connection = database();
    let first = quarantine_add(&connection, "moment", b"{}", QuarantineReason::DecodeFailed).unwrap();
    quarantine_add(&connection, "moment", b"{}", QuarantineReason::DecodeFailed).unwrap();
    assert_eq!(quarantine_oldest(&connection, QuarantineReason::DecodeFailed, 1).unwrap(), [first]);
    assert!(quarantine_oldest(&connection, QuarantineReason::MissingId, 1).unwrap().is_empty());
}

#[test]
fn entries_return_records_in_sequence_order_and_filter_by_reason() {
    let connection = database();
    assert!(quarantine_entries(&connection, QuarantineReason::DecodeFailed, 10).unwrap().is_empty());
    quarantine_add(&connection, "Message", &[1, 2], QuarantineReason::DecodeFailed).unwrap();
    quarantine_add(&connection, "Comment", &[3, 4], QuarantineReason::DecodeFailed).unwrap();
    quarantine_add(&connection, "Other", &[5], QuarantineReason::MissingId).unwrap();
    let entries = quarantine_entries(&connection, QuarantineReason::DecodeFailed, 10).unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries[0].sequence < entries[1].sequence);
    assert_eq!((entries[0].model.as_str(), entries[0].payload.as_slice()), ("Message", &[1u8, 2][..]));
    assert_eq!(entries[1].model, "Comment");
    assert!(quarantine_entries(&connection, QuarantineReason::DecodeFailed, 0).unwrap().is_empty());
}

#[test]
fn evict_over_cap_removes_oldest_plan_rejections_only() {
    let connection = database();
    assert!(evict_over_cap(&connection, QUARANTINE_CAP).unwrap().is_empty());
    let first = quarantine_add(&connection, "messages", b"first", QuarantineReason::PlanRowRejected).unwrap();
    quarantine_add(&connection, "messages", b"second", QuarantineReason::PlanRowRejected).unwrap();
    quarantine_add(&connection, "messages", b"third", QuarantineReason::PlanRowRejected).unwrap();
    assert_eq!(evict_over_cap(&connection, 2).unwrap(), [first]);
    assert_eq!(quarantine_counts(&connection).unwrap()["plan-row-rejected"], 2);
}

#[test]
fn decode_failed_entries_are_not_evicted() {
    let connection = database();
    for payload in [b"first", b"third"] {
        quarantine_add(&connection, "messages", payload, QuarantineReason::DecodeFailed).unwrap();
    }
    let before = quarantine_counts(&connection).unwrap();
    assert!(evict_over_cap(&connection, 1).unwrap().is_empty());
    assert_eq!(quarantine_counts(&connection).unwrap(), before);
}

#[test]
fn default_cap_is_500() {
    assert_eq!(QUARANTINE_CAP, 500);
    let connection = database();
    for _ in 0..501 {
        quarantine_add(&connection, "m", b"x", QuarantineReason::PlanRowRejected).unwrap();
    }
    assert_eq!(evict_over_cap(&connection, QUARANTINE_CAP).unwrap(), [1]);
}
