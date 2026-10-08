use std::collections::BTreeMap;

use crate::identity::ModelKey;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SyncContext {
    pub source: String,
    pub model: Option<ModelKey>,
    pub key: Option<String>,
    pub event: Option<String>,
    pub details: Option<BTreeMap<String, String>>,
}

impl SyncContext {
    pub fn new(source: &str) -> Self {
        Self { source: source.to_string(), ..Self::default() }
    }
}

pub fn sync_tags(context: &SyncContext) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::from([("db.source".to_string(), context.source.clone())]);
    if let Some(model) = &context.model {
        tags.insert("db.model".into(), model.as_str().to_string());
    }
    if let Some(key) = context.key.as_ref().filter(|key| !key.is_empty()) {
        tags.insert("db.key".into(), key.clone());
    }
    if let Some(event) = context.event.as_ref().filter(|event| !event.is_empty()) {
        tags.insert("db.event".into(), event.clone());
    }
    for (key, value) in context.details.iter().flatten() {
        tags.entry(format!("db.{key}")).or_insert_with(|| value.clone());
    }
    tags
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncFault {
    LandingFailed(String),
    InvalidationFailed(String),
    TrackFailed(String),
    SubscriptionPayloadMismatch,
    SubscriptionDegraded { attempts: u32 },
    UserResetDiscard { open_ops: u32 },
    RejectedRecord(String),
}

impl SyncFault {
    pub fn event_name(&self) -> &'static str {
        match self {
            SyncFault::LandingFailed(_) => "landing",
            SyncFault::InvalidationFailed(_) => "invalidation",
            SyncFault::TrackFailed(_) => "track",
            SyncFault::SubscriptionPayloadMismatch => "subscriptionPayloadMismatch",
            SyncFault::SubscriptionDegraded { .. } => "subscriptionDegraded",
            SyncFault::UserResetDiscard { .. } => "userResetDiscard",
            SyncFault::RejectedRecord(_) => "rejectedRecord",
        }
    }

    pub fn details(&self) -> Option<BTreeMap<String, String>> {
        let single = |name: &str, value: String| Some(BTreeMap::from([(name.to_string(), value)]));
        match self {
            SyncFault::LandingFailed(reason)
            | SyncFault::InvalidationFailed(reason)
            | SyncFault::TrackFailed(reason)
            | SyncFault::RejectedRecord(reason) => single("reason", reason.clone()),
            SyncFault::SubscriptionDegraded { attempts } => single("attempts", attempts.to_string()),
            SyncFault::UserResetDiscard { open_ops } => single("openOps", open_ops.to_string()),
            SyncFault::SubscriptionPayloadMismatch => None,
        }
    }

    pub fn context(&self, source: &str, model: Option<ModelKey>, key: Option<String>) -> SyncContext {
        SyncContext {
            source: source.to_string(),
            model,
            key,
            event: Some(self.event_name().to_string()),
            details: self.details(),
        }
    }
}
