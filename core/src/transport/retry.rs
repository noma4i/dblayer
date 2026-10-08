use std::time::Duration;

use super::error::TransportError;

const RETRIABLE_CODES: [&str; 4] = ["INTERNAL_SERVER_ERROR", "TIMEOUT", "SERVICE_UNAVAILABLE", "DATABASE_ERROR"];
const MAX_DELAY_SECONDS: u64 = 30;

pub fn retriable_codes() -> &'static [&'static str] {
    &RETRIABLE_CODES
}

pub fn retry_delay(attempt: u32) -> Duration {
    Duration::from_secs(2u64.saturating_pow(attempt).min(MAX_DELAY_SECONDS))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    Network,
    Server,
    Retriable,
    Fatal,
}

impl RetryClass {
    pub fn classify(is_network: bool, http_status: Option<u16>, graphql_codes: &[&str]) -> RetryClass {
        if is_network {
            return RetryClass::Network;
        }
        if http_status.is_some_and(|status| (500..=599).contains(&status)) {
            return RetryClass::Server;
        }
        if graphql_codes.iter().any(|code| RETRIABLE_CODES.contains(code)) {
            return RetryClass::Retriable;
        }
        RetryClass::Fatal
    }

    pub fn of_error(error: &TransportError) -> RetryClass {
        match error {
            TransportError::Network(_) => RetryClass::Network,
            TransportError::Http { status, .. } => RetryClass::classify(false, Some(*status), &[]),
            TransportError::Interrupted(_) | TransportError::Decode(_) | TransportError::Unscripted(_) => RetryClass::Fatal,
        }
    }

    pub fn budget(self) -> u32 {
        match self {
            RetryClass::Network => 3,
            RetryClass::Server | RetryClass::Retriable => 2,
            RetryClass::Fatal => 0,
        }
    }

    pub fn allows_retry(self, attempt: u32, is_mutation: bool) -> bool {
        !is_mutation && attempt < self.budget()
    }
}
