use std::sync::Arc;

use super::sealed_errors_tests::{moment_spec, OPERATIONS};
use super::{Config, Database, DbError, FakeTransport};

#[test]
fn data_graphql_extensions() {
    for typed in [serde_json::json!({"code":"REFUSED","transactionDisposition":"KEEP"}), serde_json::json!({})] {
        let mut fields = typed.as_object().unwrap().clone();
        fields.extend(serde_json::json!({"detail":"refused","limit":3,"context":{"values":[true,null,2]}}).as_object().unwrap().clone());
        let response = serde_json::json!({"errors":[{"message":"refused","extensions":fields}]}).to_string();
        let extensions = crate::transport::graphql_extensions(response).unwrap().unwrap();
        let encoded = serde_json::to_value(extensions).unwrap();
        assert_eq!(encoded["fields"], serde_json::json!({"detail":"\"refused\"","limit":"3","context":"{\"values\":[true,null,2]}"}));
    }
}

struct GraphqlRefusal(String);

#[async_trait::async_trait]
impl FakeTransport for GraphqlRefusal {
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> {
        Ok(self.0.clone())
    }
    async fn mutate(&self, request: super::TransportRequest) -> Result<String, DbError> { self.query(request).await }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

#[tokio::test(flavor = "multi_thread")]
async fn graphql_extensions_reach_run_and_refresh() {
    use crate::transport::{GraphqlExtensions, TransactionDisposition};
    for (fields, expected) in [
        (serde_json::json!({"code":"UNKNOWN_PRODUCT","transactionDisposition":"FINISH"}), Some(GraphqlExtensions { code: Some("UNKNOWN_PRODUCT".into()), transaction_disposition: Some(TransactionDisposition::Finish), fields: Default::default() })),
        (serde_json::json!({"code":"REACTIVATION_GRANT_MISSING","transactionDisposition":"KEEP"}), Some(GraphqlExtensions { code: Some("REACTIVATION_GRANT_MISSING".into()), transaction_disposition: Some(TransactionDisposition::Keep), fields: Default::default() })),
        (serde_json::json!({"code":"DUPLICATE_CLIENT_KEY","transactionDisposition":"LATER"}), Some(GraphqlExtensions { code: Some("DUPLICATE_CLIENT_KEY".into()), transaction_disposition: Some(TransactionDisposition::Unknown { value: "LATER".into() }), fields: Default::default() })),
        (serde_json::json!({"code":"DUPLICATE_CLIENT_KEY"}), Some(GraphqlExtensions { code: Some("DUPLICATE_CLIENT_KEY".into()), transaction_disposition: None, fields: Default::default() })),
        (serde_json::json!({"transactionDisposition":"KEEP"}), Some(GraphqlExtensions { code: None, transaction_disposition: Some(TransactionDisposition::Keep), fields: Default::default() })),
        (serde_json::json!({"code":42,"transactionDisposition":false}), None),
        (serde_json::Value::Null, None),
    ] {
        for entry in ["run", "refresh"] {
            let response = serde_json::json!({"errors":[{"message":"purchase refused","extensions":fields},{"message":"secondary","extensions":{"code":"SECONDARY","transactionDisposition":"KEEP"}}]}).to_string();
            let database = Database::open(Config { path: None }, crate::ffi::specs(serde_json::to_string(&vec![moment_spec()]).unwrap()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(Arc::new(GraphqlRefusal(response))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
            database.register_operations(OPERATIONS.into()).await.unwrap();
            database.set_online(true).await.unwrap();
            let error = match entry {
                "run" => database.run(database.session_epoch(), "MakeMoment".into(), "{}".into()).await.unwrap_err(),
                "refresh" => database.refresh(database.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into()).await.unwrap_err(),
                _ => unreachable!(),
            };
            let DbError::Graphql { extensions, .. } = error else { panic!("{entry}: {error:?}") };
            assert_eq!(extensions, expected, "{entry}: {fields}");
        }
    }
}

