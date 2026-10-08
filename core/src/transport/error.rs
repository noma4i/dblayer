#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum TransportError {
    #[error("network: {0}")]
    Network(String),
    #[error("http {status}: {body}")]
    Http { status: u16, body: String },
    #[error("decode: {0}")]
    Decode(String),
    #[error("no scripted reply for {0}")]
    Unscripted(String),
}
