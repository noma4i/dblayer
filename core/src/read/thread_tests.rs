use super::*;

#[test]
fn higher_sequence_goes_first() {
    let higher = ThreadRow::new("a").sequence(5);
    let lower = ThreadRow::new("b").sequence(3);
    assert!(higher.is_before(&lower));
    assert!(!lower.is_before(&higher));
}

#[test]
fn temp_row_goes_before_permanent_row_with_equal_sequence() {
    let temp = ThreadRow::new("a").sequence(5).temp();
    let permanent = ThreadRow::new("b").sequence(5);
    assert!(temp.is_before(&permanent));
    assert!(!permanent.is_before(&temp));
}

#[test]
fn newer_creation_goes_first_then_greater_id() {
    let newer = ThreadRow::new("a").created("2024-02");
    let older = ThreadRow::new("b").created("2024-01");
    assert!(newer.is_before(&older));
    let same_b = ThreadRow::new("b").created("2024-01");
    let same_a = ThreadRow::new("a").created("2024-01");
    assert!(same_b.is_before(&same_a));
}

#[test]
fn row_with_sequence_goes_before_row_without_one_at_equal_time() {
    let with_sequence = ThreadRow::new("a").sequence(1).created("2024-01");
    let without_sequence = ThreadRow::new("b").created("2024-01");
    assert!(with_sequence.is_before(&without_sequence));
    assert!(!without_sequence.is_before(&with_sequence));
}

#[test]
fn sorted_orders_descending_by_sequence() {
    let rows = vec![ThreadRow::new("1").sequence(1), ThreadRow::new("3").sequence(3), ThreadRow::new("2").sequence(2)];
    let ids: Vec<String> = ThreadRow::sorted(rows).into_iter().map(|row| row.id).collect();
    assert_eq!(ids, ["3", "2", "1"]);
}

#[test]
fn max_sequence_ignores_temp_rows() {
    let rows = vec![
        ThreadRow::new("a").sequence(7),
        ThreadRow::new("b").sequence(9).temp(),
        ThreadRow::new("c").sequence(4),
    ];
    assert_eq!(ThreadRow::max_sequence_number(&rows), Some(7));
    assert_eq!(ThreadRow::max_sequence_number(&[ThreadRow::new("a").sequence(9).temp()]), None);
    assert_eq!(THREAD_ORDER_FIELDS, ["sequenceNumber", "createdAt"]);
}
