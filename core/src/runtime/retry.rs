use std::time::Duration;

use super::Backoff;

#[derive(Debug, Clone, Copy)]
pub enum RetryPolicy {
    Http,
    QueryNetwork,
    QueryServer,
    Outbox,
    Upload,
    Subscription,
}

impl RetryPolicy {
    pub const fn backoff(self) -> Backoff {
        let common = Backoff::new(Duration::from_secs(1), Duration::from_secs(30)).with_jitter(0.2, 0);
        match self {
            Self::Http | Self::Subscription => common,
            Self::QueryNetwork => common.limited(3),
            Self::QueryServer => common.limited(2),
            Self::Outbox => common.limited(40),
            Self::Upload => Backoff::new(Duration::from_secs(1), Duration::from_secs(15)).with_jitter(0.5, 0).limited(50),
        }
    }
}
