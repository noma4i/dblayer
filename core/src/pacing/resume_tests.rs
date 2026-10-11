use super::*;
use std::time::Duration;

fn secs(value: u64) -> Duration {
    Duration::from_secs(value)
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
    assert_eq!(drain.chunks(&without_readers, secs(100)).unwrap(), vec![vec!["a".to_string(), "c".to_string()]]);
    assert!(waiting.chunks(&[candidate("a", 50, true)], secs(100)).unwrap().is_empty());
    assert_eq!(waiting.chunks(&[candidate("a", 50, true)], secs(110)).unwrap(), vec![vec!["a".to_string()]]);
}

#[test]
fn zero_chunk_size_fails() {
    assert_eq!(
        ResumeDrain::new(secs(60), 0).chunks(&[candidate("a", 0, true)], secs(100)),
        Err(ResumeDrainError::InvalidChunkSize(0))
    );
}

#[test]
fn defaults_are_60_seconds_and_4_per_chunk() {
    let drain = ResumeDrain::default();
    assert_eq!((drain.stale_time, drain.chunk_size), (secs(60), 4));
}
