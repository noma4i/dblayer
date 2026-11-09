use std::time::Duration;

use crate::runtime::RetryPolicy;

#[test]
fn a_policy_allows_the_sends_up_to_its_limit() {
    for (policy, limit) in [(RetryPolicy::Http, 40), (RetryPolicy::Outbox, 40), (RetryPolicy::Upload, 50), (RetryPolicy::QueryNetwork, 3), (RetryPolicy::QueryServer, 2)] {
        assert!(policy.allows(1), "{policy:?}: the first send");
        assert!(policy.allows(limit), "{policy:?}: the send {limit}");
        assert!(!policy.allows(limit + 1), "{policy:?}: the send {}", limit + 1);
    }
    for policy in [RetryPolicy::Cable, RetryPolicy::Bootstrap] {
        assert!(policy.allows(1_000_000), "{policy:?} waits for an event, not for a count");
    }
}

#[test]
fn the_wait_doubles_to_the_cap_and_jitter_only_shortens_it() {
    let plain = |attempt: u32| RetryPolicy::Http.delay_seeded(attempt, 0).as_secs_f64();
    for (attempt, ceiling) in [(1u32, 1.0), (2, 2.0), (5, 16.0), (6, 30.0), (200, 30.0)] {
        for seed in 0..32 {
            let wait = RetryPolicy::Http.delay_seeded(attempt, seed).as_secs_f64();
            assert!(wait <= ceiling && wait >= ceiling * 0.8 - 1e-9, "attempt {attempt} seed {seed}: {wait}");
        }
    }
    assert!(plain(1) <= 1.0);
    let upload = |seed| RetryPolicy::Upload.delay_seeded(10, seed);
    assert!((0..32).all(|seed| upload(seed) <= Duration::from_secs(15) && upload(seed) >= Duration::from_millis(7_500)));
}

#[test]
fn one_seed_gives_one_wait_and_two_seeds_give_two() {
    assert_eq!(RetryPolicy::Outbox.delay_seeded(3, 7), RetryPolicy::Outbox.delay_seeded(3, 7));
    assert_ne!(RetryPolicy::Outbox.delay_seeded(3, 7), RetryPolicy::Outbox.delay_seeded(3, 8));
}
