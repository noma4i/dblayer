use super::retention::RetentionPool;

#[test]
fn retained_values_share_one_byte_budget_across_models() {
    let pool = RetentionPool::new();
    let first = pool.keep("Note".into(), "same".into(), 5 * 1024 * 1024, 0).selected.unwrap();
    let second = pool.keep("Message".into(), "same".into(), 5 * 1024 * 1024, 1);
    assert_eq!(second.evicted, vec![first]);
    assert!(pool.take("Note".into(), "same".into(), 2).selected.is_none());
    assert_eq!(pool.take("Message".into(), "same".into(), 2).selected, second.selected);
}

#[test]
fn retention_removes_only_expired_values_and_caps_each_model() {
    let pool = RetentionPool::new();
    let old = pool.keep("Note".into(), "old".into(), 1, 0).selected.unwrap();
    let fresh = pool.keep("Message".into(), "fresh".into(), 1, 100).selected.unwrap();
    let (expired, expired_count) = pool.count("Message", 120_000);
    assert_eq!(expired.evicted, vec![old]);
    assert_eq!(expired_count, 1);
    assert_eq!(pool.take("Message".into(), "fresh".into(), 120_001).selected, Some(fresh));
    let oldest = pool.keep("Note".into(), "0".into(), 1, 120_001).selected.unwrap();
    for index in 1..200 {
        assert!(pool.keep("Note".into(), index.to_string(), 1, 120_001).evicted.is_empty());
    }
    let change = pool.keep("Note".into(), "200".into(), 1, 120_001);
    assert_eq!(change.evicted, vec![oldest]);
    assert_eq!(pool.count("Note", 120_001).1, 200);
    assert_eq!(pool.clear("Note".into()).evicted.len(), 200);
    assert_eq!(pool.count("Note", 120_001).1, 0);
}
