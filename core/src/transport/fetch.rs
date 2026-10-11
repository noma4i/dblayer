use crate::identity::Generation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    Transport(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchReport {
    Loaded,
    KeptStale,
    Failed(FetchError),
}

impl FetchReport {
    pub fn of(is_forced: bool, has_window: bool, failure: Option<&str>) -> FetchReport {
        let Some(failure) = failure else { return FetchReport::Loaded };
        if !is_forced && has_window {
            return FetchReport::KeptStale;
        }
        FetchReport::Failed(FetchError::Transport(failure.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchAttempt {
    pub key: String,
    pub issued: i64,
    pub generation: Generation,
    pub base_revision: i64,
}

impl FetchAttempt {
    pub fn new(key: impl Into<String>, issued: i64, generation: Generation, base_revision: i64) -> Self {
        Self { key: key.into(), issued, generation, base_revision }
    }

    pub fn is_superseded_by(&self, other: &FetchAttempt) -> bool {
        self.key == other.key && other.issued > self.issued
    }

    pub fn applies_to(&self, current: Generation) -> bool {
        self.generation == current
    }
}
