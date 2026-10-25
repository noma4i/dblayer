use std::time::Duration;

use super::*;

const TICK: Duration = Duration::from_millis(100);

#[test]
fn sent_is_the_sum_of_finished_and_running_parts() {
    let mut progress = TransferProgress::new(100, TICK);
    progress.begin_phase([(1, 30)]);
    progress.report(2, 10);
    progress.report(3, 5);
    assert_eq!(progress.sent(), 45);
    progress.finish(2, 30);
    assert_eq!(progress.sent(), 65);
}

#[test]
fn inv_upload_08_a_restarted_part_never_lowers_the_total_inside_a_phase() {
    let mut progress = TransferProgress::new(100, TICK);
    progress.begin_phase([]);
    progress.report(1, 40);
    assert_eq!(progress.sent(), 40);
    progress.report(1, 0);
    assert_eq!(progress.sent(), 40);
    progress.report(1, 50);
    assert_eq!(progress.sent(), 50);
}

#[test]
fn a_new_phase_resets_to_the_finished_baseline_and_sent_is_capped_by_the_total() {
    let mut progress = TransferProgress::new(100, TICK);
    progress.begin_phase([]);
    progress.report(1, 90);
    assert_eq!(progress.sent(), 90);
    progress.begin_phase([(1, 20)]);
    assert_eq!(progress.sent(), 20);
    progress.report(2, 500);
    assert_eq!(progress.sent(), 100);
}

#[test]
fn ticks_are_throttled_by_the_clock() {
    let mut progress = TransferProgress::new(100, TICK);
    progress.begin_phase([]);
    progress.report(1, 10);
    assert_eq!(progress.due(Duration::from_millis(1000)), Some(10));
    progress.report(1, 20);
    assert_eq!(progress.due(Duration::from_millis(1050)), None);
    assert_eq!(progress.due(Duration::from_millis(1100)), Some(20));
}
