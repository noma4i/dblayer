#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    #[error("security {code:?}: {detail}")]
    Security { code: super::SecurityCode, detail: String },
    #[error("config {code:?}: {detail}")]
    Config { code: super::ConfigCode, detail: String },
    #[error("network: {0}")]
    Network(String),
    #[error("http {status}: {body}")]
    Http { status: u16, body: String },
    #[error("request is {size} bytes, exceeding the local limit of {limit} bytes")]
    RequestTooLarge { size: u64, limit: u64 },
    #[error("the answer is above the limit of {limit} bytes")]
    ResponseTooLarge { limit: u64 },
    #[error("the answer of a sent request is above the limit of {limit} bytes")]
    SentAnswerTooLarge { limit: u64 },
    #[error("graphql: {}", messages.join("\n"))]
    Graphql { messages: Vec<String>, extensions: Option<super::GraphqlExtensions> },
    #[error("graphql answer: {response:?}")]
    GraphqlAnswer { response: Box<super::Response> },
    #[error("session {code:?}: {detail}")]
    Session { code: super::SessionCode, detail: String },
    #[error("interrupted: {0}")]
    Interrupted(String),
    #[error("the session ended")]
    NoSession,
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("no scripted reply for {0}")]
    Unscripted(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("internal: {0}")]
    Internal(String),
}

impl TransportError {
    pub(crate) fn graphql_answer(code: String, body: String) -> Self {
        match serde_json::from_str::<super::Response>(&body) {
            Ok(response) if !response.errors.is_empty() => Self::GraphqlAnswer { response: Box::new(response) },
            _ => Self::Graphql { messages: vec![code.clone()], extensions: Some(super::GraphqlExtensions { code: Some(code), transaction_disposition: None, fields: Default::default() }) },
        }
    }

    pub fn refusal(code: &str) -> Self {
        match super::answer_kind::refusal_code(code.to_string()) {
            super::Refusal::Session(session) => Self::Session { code: session, detail: code.to_string() },
            super::Refusal::Graphql(name) => Self::Graphql { messages: vec![name], extensions: None },
        }
    }
}

impl From<crate::store::CoreError> for TransportError {
    fn from(error: crate::store::CoreError) -> Self { TransportError::Storage(format!("session end: {error}")) }
}
