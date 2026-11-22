use crate::cloud::{HybridClock, CloudStamp};

#[test]
fn cloud_stamps_follow_remote_and_durable_time_when_the_wall_clock_moves_back() {
    let replica = [1; 16];
    let past = CloudStamp { millis: 1000, counter: 2, replica };
    let remote = CloudStamp { millis: 1050, counter: 4, replica: [2; 16] };
    let mut clock = HybridClock::new(replica, Some(past));
    assert_eq!(clock.tick(900), Some(CloudStamp { counter: 3, ..past }));
    clock.observe(remote);
    clock.observe(past);
    let after_remote = clock.tick(900).unwrap();
    assert_eq!(after_remote, CloudStamp { millis: 1050, counter: 5, replica });
    assert!(after_remote > remote);
    let mut reopened = HybridClock::new(replica, Some(after_remote));
    assert_eq!(reopened.tick(800), Some(CloudStamp { counter: 6, ..after_remote }));
    assert_eq!(reopened.tick(1100), Some(CloudStamp { millis: 1100, counter: 0, replica }));

    let mut overflow = HybridClock::new(replica, Some(CloudStamp { counter: u32::MAX, ..past }));
    assert_eq!(overflow.tick(1000), Some(CloudStamp { millis: 1001, counter: 0, replica }));
    let mut exhausted = HybridClock::new(replica, Some(CloudStamp { millis: u64::MAX, counter: u32::MAX, replica }));
    assert_eq!(exhausted.tick(0), None);
}
