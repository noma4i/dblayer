use std::sync::Arc;
use crate::ffi::{Config, Database, Transport};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;

impl Database {
    fn create_unsent(&self, model: &str, values_json: &str) {
        let values = serde_json::from_str(values_json).unwrap();
        self.db.create_in(&self.db.admit().unwrap(), None, model, values, Default::default()).unwrap();
    }
}

struct Recorder(std::sync::Mutex<Vec<String>>);
#[async_trait::async_trait]
impl Transport for Recorder {
    async fn open_cable(&self, _: Arc<crate::ffi::CableLease>) -> Result<(), crate::ffi::DbError> { Err(crate::ffi::DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), crate::ffi::DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: crate::ffi::TransportRequest) -> Result<String, crate::ffi::DbError> { unreachable!() }
    async fn mutate(&self, request: crate::ffi::TransportRequest) -> Result<String, crate::ffi::DbError> {
        self.0.lock().unwrap().push(request.operation_name);
        Ok(r#"{"data":{"createMoment":{"id":"m1","text":"hello"}}}"#.into())
    }
    async fn subscribe(&self, _: crate::ffi::TransportRequest, _: Arc<crate::ffi::SubscriptionSink>) -> Result<(), crate::ffi::DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::ffi::PutFile, _: String, _: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, crate::ffi::DbError> { unreachable!() }
}

#[tokio::test]
async fn a_create_left_pending_by_the_last_session_waits_for_registration_after_open() {
    use crate::schema::{ActionDeclaration, RootShape};
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("CreateMoment".into());
    create.durable = true;
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.actions.push(create);
    let models = serde_json::to_string(&vec![model]).unwrap();
    let folder = tempfile::tempdir().unwrap();
    let path = folder.path().join("db.sqlite").to_string_lossy().into_owned();
    let config = || Config { path: Some(path.clone()), data_version: None, cache_rows: 32 };
    let first = Database::open(config(), models.clone(), None::<Arc<dyn Transport>>, None, None, None, None, None).await.unwrap();
    first.create_unsent("Moment", r#"{"text":"hello"}"#);
    drop(first);

    let transport = Arc::new(Recorder(Default::default()));
    let second = Database::open(config(), models, Some(transport.clone() as Arc<dyn Transport>), None, None, None, None, None).await.unwrap();
    second.maintain().await.unwrap();
    assert!(transport.0.lock().unwrap().is_empty());
    second.register_operations(r#"[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id text } }"}]"#.into()).unwrap();
    second.flush().await.unwrap();
    assert_eq!(*transport.0.lock().unwrap(), ["CreateMoment"]);
}
