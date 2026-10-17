use serde_json::Value;

use crate::transport::{GraphQLRequest, ResponseOutcome};

use super::error::SyncError;
use super::{locked, Sync};

impl Sync {
    /// Sends one registered operation and returns the value at the root field of its answer. Nothing is queued, stored or retried.
    pub async fn run(&self, operation: &str, variables: Value) -> Result<Value, SyncError> {
        let definition = locked(&self.inner.operations).get(operation).cloned().ok_or_else(|| SyncError::UnknownOperation(operation.to_string()))?;
        if !self.inner.connectivity.is_online() {
            return Err(SyncError::Offline);
        }
        let request = GraphQLRequest::new(&definition.document, operation, variables);
        let transport = &self.inner.transport;
        let response = if definition.document.trim_start().starts_with("mutation") { transport.mutate(request).await } else { transport.query(request).await }
            .map_err(SyncError::Transport)?;
        if response.shape().outcome() != ResponseOutcome::Commit {
            let messages: Vec<String> = response.errors.iter().map(|error| error.message.clone()).collect();
            return Err(SyncError::Graphql { messages: if messages.is_empty() { vec!["empty response".to_string()] } else { messages }, extensions: response.extensions() });
        }
        let data = response.data.unwrap_or(Value::Null);
        Ok(data.get(&definition.root_path).cloned().unwrap_or(Value::Null))
    }
}
