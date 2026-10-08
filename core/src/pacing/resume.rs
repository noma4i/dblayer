use std::collections::HashMap;
use std::time::Duration;

pub fn effective_window(stale_time: Duration, resume_stale_time: Option<Duration>, has_resume_override: bool) -> Option<Duration> {
    if has_resume_override {
        resume_stale_time
    } else {
        Some(stale_time)
    }
}

pub fn refetches_on_resume(last_fetched_at: Duration, now: Duration, window: Option<Duration>) -> bool {
    window.is_some_and(|window| now.saturating_sub(last_fetched_at) >= window)
}

pub fn resume_candidates<'a>(
    keys: &[&'a str],
    last_fetched_at: &HashMap<String, Duration>,
    now: Duration,
    window: Option<Duration>,
) -> Vec<&'a str> {
    keys.iter()
        .copied()
        .filter(|key| match last_fetched_at.get(*key) {
            Some(fetched_at) => refetches_on_resume(*fetched_at, now, window),
            None => true,
        })
        .collect()
}

pub fn skips_without_readers(reader_count: usize) -> bool {
    reader_count == 0
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleScopeCandidate {
    pub key: String,
    pub last_fetched_at: Duration,
    pub has_readers: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResumeDrainError {
    #[error("chunk size must be at least 1, got {0}")]
    InvalidChunkSize(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumeDrain {
    pub stale_time: Duration,
    pub chunk_size: usize,
}

impl Default for ResumeDrain {
    fn default() -> Self {
        Self { stale_time: Duration::from_secs(60), chunk_size: 4 }
    }
}

impl ResumeDrain {
    pub fn new(stale_time: Duration, chunk_size: usize) -> Self {
        Self { stale_time, chunk_size }
    }

    pub fn chunks(&self, candidates: &[StaleScopeCandidate], now: Duration) -> Result<Vec<Vec<String>>, ResumeDrainError> {
        if self.chunk_size < 1 {
            return Err(ResumeDrainError::InvalidChunkSize(self.chunk_size));
        }
        let eligible: Vec<String> = candidates
            .iter()
            .filter(|candidate| candidate.has_readers && now.saturating_sub(candidate.last_fetched_at) >= self.stale_time)
            .map(|candidate| candidate.key.clone())
            .collect();
        Ok(eligible.chunks(self.chunk_size).map(<[String]>::to_vec).collect())
    }

    pub fn stops_after_reset(chunks: &[Vec<String>], completed_chunks: usize) -> Vec<Vec<String>> {
        chunks.iter().take(completed_chunks).cloned().collect()
    }

    pub fn total_keys(chunks: &[Vec<String>]) -> usize {
        chunks.iter().map(Vec::len).sum()
    }
}
