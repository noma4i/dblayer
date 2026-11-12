use super::*;
use std::time::Duration;

#[test]
fn system_clock_reports_time_since_epoch() {
    assert!(SystemClock.now() > Duration::from_secs(1_700_000_000));
}
