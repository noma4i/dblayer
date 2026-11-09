use super::*;

#[test]
fn a_second_acquire_is_refused_until_the_first_lease_is_released_or_dropped() {
    let slot = OpenSlot::new();
    let first = slot.acquire().unwrap();
    match slot.acquire() {
        Err(DbError::Invalid { detail }) => assert_eq!(detail, "database is already open; open it once at app start"),
        other => panic!("second acquire gave {:?}", other.map(|_| ())),
    }
    first.release();
    first.release();
    let second = slot.acquire().unwrap();
    assert!(matches!(slot.acquire(), Err(DbError::Invalid { .. })));
    drop(second);
    assert!(slot.acquire().is_ok());
}
