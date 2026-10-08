use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown freshness policy {0}")]
pub struct UnknownFreshnessPolicy(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshnessPolicy {
    pub name: String,
    pub duration: Duration,
}

impl FreshnessPolicy {
    pub fn new(name: impl Into<String>, duration: Duration) -> Self {
        Self { name: name.into(), duration }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FreshnessTable {
    durations: HashMap<String, Duration>,
}

impl FreshnessTable {
    pub fn new(policies: impl IntoIterator<Item = FreshnessPolicy>) -> Self {
        Self { durations: policies.into_iter().map(|policy| (policy.name, policy.duration)).collect() }
    }

    pub fn resolve(&self, name: &str) -> Result<Duration, UnknownFreshnessPolicy> {
        self.durations.get(name).copied().ok_or_else(|| UnknownFreshnessPolicy(name.to_string()))
    }

    pub fn missing<'a>(&self, required: &[&'a str]) -> Vec<&'a str> {
        required.iter().copied().filter(|name| !self.durations.contains_key(*name)).collect()
    }

    pub fn is_stale(&self, last_fetched_at: Duration, now: Duration, name: &str) -> Result<bool, UnknownFreshnessPolicy> {
        Ok(now.saturating_sub(last_fetched_at) >= self.resolve(name)?)
    }
}

pub fn selected_policy<'a>(is_empty_result: bool, landed: &'a str, empty: Option<&'a str>) -> &'a str {
    match empty {
        Some(empty) if is_empty_result => empty,
        _ => landed,
    }
}

pub fn is_fresh(landed_at: Duration, now: Duration, window: Duration) -> bool {
    now.saturating_sub(landed_at) < window
}

pub fn expires_at(landed_at: Duration, window: Duration) -> Duration {
    landed_at + window
}
