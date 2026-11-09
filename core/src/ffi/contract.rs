//! The server contract for the test doubles of the platforms: they script tokens, the core decides.

use std::sync::{Arc, Mutex};

use crate::server_contract::{Admission, Contract, Request};

#[derive(uniffi::Object)]
pub struct ServerContract {
    inner: Mutex<Contract<String>>,
}

#[derive(uniffi::Enum)]
pub enum ContractAdmission {
    /// The pair was answered before with the same body: the token of that answer.
    Stored { token: String },
    /// The server refuses the request without a sequence: the answer body.
    Refused { body: String },
    /// The pair came again with another body.
    TwoBodies,
    /// The server executes the request: take a scripted token and settle it.
    Fresh,
}

fn request_of(scope: &str, request_json: &str) -> Request {
    Request::from_wire(scope, &serde_json::from_str(request_json).unwrap_or_default())
}

impl ServerContract {
    fn contract(&self) -> std::sync::MutexGuard<'_, Contract<String>> { self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner) }
}

#[uniffi::export]
impl ServerContract {
    /// A server that knows its session number: the first sequenced request must carry 1.
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> { Arc::new(Self { inner: Mutex::new(Contract::new()) }) }

    /// A server that takes the number of the first sequenced request as its session number.
    #[uniffi::constructor]
    pub fn adopting() -> Arc<Self> { Arc::new(Self { inner: Mutex::new(Contract::adopting()) }) }

    pub fn register(&self, hash: String) { self.contract().register(&hash) }
    pub fn forget(&self) { self.contract().forget() }
    pub fn cache_everything(&self) { self.contract().cache_everything() }
    pub fn set_last(&self, sequence: i64) { self.contract().set_last(sequence) }
    pub fn last(&self) -> i64 { self.contract().last() }
    pub fn script(&self, operation: String, token: String) { self.contract().script(&operation, token) }
    pub fn standing(&self, token: String) { self.contract().standing(token) }

    /// The next scripted token of the operation. Nothing scripted: `None`, the double fails the test.
    pub fn take(&self, operation: String) -> Option<String> { self.contract().take(&operation) }

    /// `request_json`: `{"requestId": text, "sequence": number or null, "body": {"operationName", "variables", "query"?, "extensions"?}}`.
    pub fn admit(&self, scope: String, request_json: String) -> ContractAdmission {
        match self.contract().admit(&request_of(&scope, &request_json)) {
            Admission::Stored(token) => ContractAdmission::Stored { token },
            Admission::Refused(body) => ContractAdmission::Refused { body: body.to_string() },
            Admission::TwoBodies => ContractAdmission::TwoBodies,
            Admission::Fresh => ContractAdmission::Fresh,
        }
    }

    /// The answer of the token reached the client: the pair is remembered and a sequence is spent.
    pub fn settle(&self, scope: String, request_json: String, token: String) { self.contract().settle(&request_of(&scope, &request_json), token) }
}
