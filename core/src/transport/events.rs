use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventFault {
    EmptyName,
    ReservedName,
    DebounceWithoutMergeKey,
    NoRootWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishVerdict {
    Publish,
    SkipPlanningError,
    SkipApplyError,
    SkipStaleGeneration,
    SkipNoRootWrite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSpec {
    pub name: String,
    pub writes_root: bool,
    pub debounce_interval: Option<Duration>,
    pub has_merge_key: bool,
}

impl EventSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), writes_root: true, debounce_interval: None, has_merge_key: false }
    }

    pub fn debounced(mut self, interval: Duration) -> Self {
        self.debounce_interval = Some(interval);
        self
    }

    pub fn merged_by_row(mut self) -> Self {
        self.has_merge_key = true;
        self
    }

    pub fn without_root_write(mut self) -> Self {
        self.writes_root = false;
        self
    }

    pub fn fault(&self) -> Option<EventFault> {
        if self.name.is_empty() {
            return Some(EventFault::EmptyName);
        }
        if self.name.starts_with('$') {
            return Some(EventFault::ReservedName);
        }
        if self.debounce_interval.is_some() && !self.has_merge_key {
            return Some(EventFault::DebounceWithoutMergeKey);
        }
        (!self.writes_root).then_some(EventFault::NoRootWrite)
    }

    pub fn verdict(&self, planning_failed: bool, apply_failed: bool, is_stale_generation: bool) -> PublishVerdict {
        if planning_failed {
            return PublishVerdict::SkipPlanningError;
        }
        if apply_failed {
            return PublishVerdict::SkipApplyError;
        }
        if is_stale_generation {
            return PublishVerdict::SkipStaleGeneration;
        }
        if !self.writes_root {
            return PublishVerdict::SkipNoRootWrite;
        }
        PublishVerdict::Publish
    }

    pub fn publishes_after_commit(&self) -> bool {
        self.writes_root
    }

    pub fn bucket_key(&self, row_id: &str) -> String {
        if self.has_merge_key {
            format!("{}:{}", self.name, row_id)
        } else {
            self.name.clone()
        }
    }
}

pub fn routable(specs: &[EventSpec]) -> Vec<EventSpec> {
    specs.iter().filter(|spec| spec.fault().is_none()).cloned().collect()
}

pub fn event_names(specs: &[EventSpec]) -> Vec<&str> {
    specs.iter().map(|spec| spec.name.as_str()).collect()
}

pub fn max_debounce(specs: &[EventSpec]) -> Option<Duration> {
    specs.iter().filter_map(|spec| spec.debounce_interval).max()
}
