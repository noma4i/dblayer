use serde_json::Value;
use super::EncryptedConfig;

mod link;
mod keys;
mod frame;
mod schema;
mod session;
pub use session::CableSession;
mod wire;
pub use wire::{CableConnect, CableSocket, CableSubscriptions, SocketEvent};
#[cfg(test)]
mod session_tests;
#[cfg(test)]
mod wire_tests;
pub use link::CableLink;

const CLIENT_BYTES: usize = 32 * 1024;
const SERVER_BYTES: usize = 256 * 1024;
const FRAME_BUDGET: u64 = 1 << 20;
const HEADER: &str = r#"{"alg":"dir","enc":"A256GCM"}"#;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CableError {
    #[error("untrusted cable server")]
    ServerKeyMismatch,
    #[error("invalid cable challenge")]
    Challenge,
    #[error("invalid cable frame")]
    Frame,
    #[error("cable frame sequence mismatch")]
    Sequence,
    #[error("cable frame exceeds byte limit")]
    TooLarge,
    #[error("cable key exhausted")]
    Exhausted,
    #[error("cable is closed")]
    Closed,
}

#[cfg(test)]
mod link_tests;
