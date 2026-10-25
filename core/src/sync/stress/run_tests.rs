use std::time::Duration;

use super::{run, runtime, settle_blocking, shrink, StressConfig};

#[test]
fn finishing_a_run_keeps_another_runs_uploads() {
    let first = super::scratch_dir();
    let second = super::scratch_dir();
    let upload = second.path().join("upload.bin");
    std::fs::write(&upload, b"upload").unwrap();
    drop(first);
    assert_eq!(std::fs::read(&upload).unwrap(), b"upload");
}

#[test]
fn shrink_returns_the_smallest_operation_count_that_still_fails() {
    assert_eq!(shrink(500, |ops| ops >= 37, Duration::from_secs(5)), Some(37));
    assert_eq!(shrink(500, |ops| ops >= 500, Duration::from_secs(5)), Some(500));
    assert_eq!(shrink(500, |_| false, Duration::from_secs(5)), None);
}

#[test]
fn one_thread_and_one_seed_end_in_the_same_state() {
    let config = StressConfig { seed: 7, threads: 1, seconds: 60, ops: Some(150), trace: false };
    let first = run(&config);
    let second = run(&config);
    assert_eq!(first.digest, second.digest);
    assert_eq!(first.ops, [150]);
}

#[test]
fn another_seed_ends_in_another_state() {
    let first = run(&StressConfig { seed: 7, threads: 1, seconds: 60, ops: Some(150), trace: false });
    let other = run(&StressConfig { seed: 8, threads: 1, seconds: 60, ops: Some(150), trace: false });
    assert_ne!(first.digest, other.digest);
}

#[test]
fn settling_waits_for_a_blocking_job_until_it_is_released() {
    runtime(1).block_on(async {
        let (release, held) = std::sync::mpsc::channel::<()>();
        let job = tokio::task::spawn_blocking(move || held.recv().unwrap());
        let settled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = settled.clone();
        let waiter = tokio::spawn(async move {
            settle_blocking(String::new).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(!settled.load(std::sync::atomic::Ordering::SeqCst));
        release.send(()).unwrap();
        job.await.unwrap();
        waiter.await.unwrap();
        assert!(settled.load(std::sync::atomic::Ordering::SeqCst));
    });
}
