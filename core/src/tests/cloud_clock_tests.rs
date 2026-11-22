use crate::cloud::{HybridClock, Stamp};

#[test]
fn cloud_stamps_follow_remote_and_durable_time_when_the_wall_clock_moves_back() {
    let replica = [1; 16];
    let past = Stamp { millis: 1000, counter: 2, replica };
    let remote = Stamp { millis: 1050, counter: 4, replica: [2; 16] };
    let mut clock = HybridClock::new(replica, Some(past));
    assert_eq!(clock.tick(900), Some(Stamp { counter: 3, ..past }));
    clock.observe(remote);
    clock.observe(past);
    let after_remote = clock.tick(900).unwrap();
    assert_eq!(after_remote, Stamp { millis: 1050, counter: 5, replica });
    assert!(after_remote > remote);
    let mut reopened = HybridClock::new(replica, Some(after_remote));
    assert_eq!(reopened.tick(800), Some(Stamp { counter: 6, ..after_remote }));
    assert_eq!(reopened.tick(1100), Some(Stamp { millis: 1100, counter: 0, replica }));

    let mut overflow = HybridClock::new(replica, Some(Stamp { counter: u32::MAX, ..past }));
    assert_eq!(overflow.tick(1000), Some(Stamp { millis: 1001, counter: 0, replica }));
    let mut exhausted = HybridClock::new(replica, Some(Stamp { millis: u64::MAX, counter: u32::MAX, replica }));
    assert_eq!(exhausted.tick(0), None);
}
