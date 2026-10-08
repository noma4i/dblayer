use super::*;
use std::collections::HashMap;
use std::time::Duration;

fn secs(value: u64) -> Duration {
    Duration::from_secs(value)
}

#[test]
fn effective_window_uses_stale_time_or_resume_override_and_refetch_needs_expired_window() {
    assert_eq!(effective_window(secs(30), None, false), Some(secs(30)));
    assert_eq!(effective_window(secs(30), None, true), None);
    assert_eq!(effective_window(secs(30), Some(secs(10)), true), Some(secs(10)));
    assert!(!refetches_on_resume(secs(0), secs(30), None));
    assert!(refetches_on_resume(secs(0), secs(30), Some(secs(30))));
    assert!(!refetches_on_resume(secs(0), secs(29), Some(secs(30))));
}

#[test]
fn candidates_include_unknown_and_expired_keys_and_skip_without_readers() {
    let landed = HashMap::from([("a".to_string(), secs(25))]);
    let result = resume_candidates(&["a", "b"], &landed, secs(30), Some(secs(30)));
    assert_eq!(result, vec!["b"]);
    assert!(skips_without_readers(0));
    assert!(!skips_without_readers(1));
}

#[test]
fn candidates_with_disabled_window_still_include_unknown_keys_only() {
    let landed = HashMap::from([("a".to_string(), secs(0))]);
    assert_eq!(resume_candidates(&["a", "b"], &landed, secs(999), None), vec!["b"]);
}

fn candidate(key: &str, last_fetched_at: u64, has_readers: bool) -> StaleScopeCandidate {
    StaleScopeCandidate { key: key.into(), last_fetched_at: secs(last_fetched_at), has_readers }
}

#[test]
fn chunks_split_candidates_exclude_without_readers_and_wait_for_stale_time() {
    let candidates: Vec<_> = (0..10).map(|index| candidate(&format!("k{index}"), 0, true)).collect();
    let without_readers = [candidate("a", 0, true), candidate("b", 0, false), candidate("c", 0, true), candidate("d", 0, false)];
    let drain = ResumeDrain::default();
    let waiting = ResumeDrain::new(secs(60), 4);

    let result = drain.chunks(&candidates, secs(100)).unwrap();

    assert_eq!(result.iter().map(Vec::len).collect::<Vec<_>>(), vec![4, 4, 2]);
    assert_eq!(ResumeDrain::total_keys(&result), 10);
    assert_eq!(drain.chunks(&without_readers, secs(100)).unwrap(), vec![vec!["a".to_string(), "c".to_string()]]);
    assert!(waiting.chunks(&[candidate("a", 50, true)], secs(100)).unwrap().is_empty());
    assert_eq!(waiting.chunks(&[candidate("a", 50, true)], secs(110)).unwrap(), vec![vec!["a".to_string()]]);
}

#[test]
fn zero_chunk_size_fails_and_stops_after_reset_limits_chunks() {
    let candidates: Vec<_> = (0..10).map(|index| candidate(&format!("k{index}"), 0, true)).collect();
    let drain = ResumeDrain::default();
    let chunks = drain.chunks(&candidates, secs(100)).unwrap();

    assert_eq!(
        ResumeDrain::new(secs(60), 0).chunks(&[candidate("a", 0, true)], secs(100)),
        Err(ResumeDrainError::InvalidChunkSize(0))
    );
    assert_eq!(ResumeDrain::stops_after_reset(&chunks, 1).len(), 1);
    assert!(ResumeDrain::stops_after_reset(&chunks, 0).is_empty());
    assert_eq!(ResumeDrain::stops_after_reset(&chunks, 9).len(), 3);
}

#[test]
fn defaults_are_60_seconds_and_4_per_chunk() {
    let drain = ResumeDrain::default();
    assert_eq!((drain.stale_time, drain.chunk_size), (secs(60), 4));
}
