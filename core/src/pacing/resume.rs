use std::time::Duration;

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
    #[cfg(test)]
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
}
