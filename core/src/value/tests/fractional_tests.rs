use super::*;

fn fraction(key: &str) -> &str {
    let head = key.as_bytes()[0];
    let integer = if head.is_ascii_lowercase() { usize::from(head - b'a') + 2 } else { usize::from(b'Z' - head) + 2 };
    &key[integer..]
}

fn ordered(case: &str, keys: &[String]) {
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]), "{case}: keys out of order");
    assert!(keys.iter().all(|key| !key.is_empty() && !fraction(key).ends_with('0')), "{case}: empty key or a fraction that ends with zero");
}

fn longest(keys: &[String]) -> usize {
    keys.iter().map(String::len).max().unwrap_or_default()
}

#[test]
fn keys_between_any_two_neighbours_keep_the_order() {
    let mut keys: Vec<String> = Vec::new();
    let mut state = 37_u64;
    for _ in 0..3000 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let index = (state >> 33) as usize % (keys.len() + 1);
        let lower = index.checked_sub(1).map(|before| keys[before].as_str());
        let upper = keys.get(index).map(String::as_str);
        keys.insert(index, fractional_between(lower, upper).unwrap());
    }
    ordered("random positions", &keys);
}

#[test]
fn appends_and_prepends_grow_with_the_logarithm_of_their_count() {
    let mut tail: Vec<String> = Vec::new();
    let mut head: Vec<String> = Vec::new();
    for _ in 0..20_000 {
        tail.push(fractional_between(tail.last().map(String::as_str), None).unwrap());
        head.push(fractional_between(None, head.last().map(String::as_str)).unwrap());
    }
    head.reverse();
    ordered("20000 appends", &tail);
    ordered("20000 prepends", &head);
    assert!(longest(&tail) <= 4 && longest(&head) <= 4, "longest key: {} after appends, {} after prepends", longest(&tail), longest(&head));
}

#[test]
fn repeated_inserts_at_one_place_grow_by_one_character_in_a_few_inserts() {
    let upper = fractional_between(None, None).unwrap();
    let mut keys = vec![fractional_between(None, Some(&upper)).unwrap()];
    for _ in 0..120 {
        keys.push(fractional_between(keys.last().map(String::as_str), Some(&upper)).unwrap());
    }
    ordered("120 inserts before one upper bound", &keys);
    assert!(longest(&keys) > ORDER_KEY_LIMIT / 2 && longest(&keys) < 4 * ORDER_KEY_LIMIT, "longest key: {}", longest(&keys));
}

#[test]
fn a_run_of_keys_between_two_bounds_is_ordered_and_short() {
    for (lower, upper) in [(None, None), (Some("a0"), None), (None, Some("a0")), (Some("a0"), Some("a1")), (Some("a0V"), Some("a0W"))] {
        let keys = fractional_keys_between(lower, upper, 17).unwrap();
        assert_eq!(keys.len(), 17, "{lower:?} {upper:?}");
        ordered("run", &keys);
        assert!(lower.is_none_or(|bound| keys[0].as_str() > bound) && upper.is_none_or(|bound| keys[16].as_str() < bound), "{lower:?} {upper:?}: {keys:?}");
        assert!(longest(&keys) <= lower.map_or(0, str::len).max(upper.map_or(0, str::len)) + 4, "{lower:?} {upper:?}: {keys:?}");
    }
}

#[test]
fn bounds_out_of_order_or_not_keys_are_refused() {
    for (lower, upper) in [(Some("a1"), Some("a0")), (Some("a1"), Some("a1")), (Some("a10"), Some("a2")), (Some("!"), None), (None, Some("a00"))] {
        assert_eq!(fractional_between(lower, upper), Err(OrderKeyError::InvalidBounds), "{lower:?} {upper:?}");
    }
}
