#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    #[error("network: {0}")]
    Network(String),
    #[error("http {status}: {body}")]
    Http { status: u16, body: String },
    #[error("graphql: {}", messages.join("\n"))]
    Graphql { messages: Vec<String> },
    #[error("interrupted: {0}")]
    Interrupted(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("no scripted reply for {0}")]
    Unscripted(String),
}
