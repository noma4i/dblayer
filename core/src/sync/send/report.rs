#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    NotBound,
    BlockedByHead,
    Offline,
    Waiting,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendResult {
    Committed,
    Rejected(String),
    DeliveryUnknown,
    Deferred,
    Quarantined,
    Skipped(SkipReason),
    Discarded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentOperation {
    pub sequence: i64,
    pub action_key: String,
    pub result: SendResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FlushReport {
    pub operations: Vec<SentOperation>,
}

impl FlushReport {
    pub fn committed(&self) -> usize {
        self.operations.iter().filter(|sent| sent.result == SendResult::Committed).count()
    }

    pub fn rejections(&self) -> Vec<&str> {
        self.operations
            .iter()
            .filter_map(|sent| match &sent.result {
                SendResult::Rejected(message) => Some(message.as_str()),
                _ => None,
            })
            .collect()
    }
}
