use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventFault {
    EmptyName,
    ReservedName,
    DebounceWithoutMergeKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSpec {
    pub name: String,
    pub debounce_interval: Option<Duration>,
    pub has_merge_key: bool,
}

impl EventSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), debounce_interval: None, has_merge_key: false }
    }

    pub fn debounced(mut self, interval: Duration) -> Self {
        self.debounce_interval = Some(interval);
        self
    }

    pub fn merged_by_row(mut self) -> Self {
        self.has_merge_key = true;
        self
    }

    pub fn fault(&self) -> Option<EventFault> {
        if self.name.is_empty() {
            return Some(EventFault::EmptyName);
        }
        if self.name.starts_with('$') {
            return Some(EventFault::ReservedName);
        }
        (self.debounce_interval.is_some() && !self.has_merge_key).then_some(EventFault::DebounceWithoutMergeKey)
    }

    pub fn bucket_key(&self, row_id: &str) -> String {
        if self.has_merge_key {
            format!("{}:{}", self.name, row_id)
        } else {
            self.name.clone()
        }
    }
}
