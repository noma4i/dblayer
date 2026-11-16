use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use super::{Config, Database, DbError, FakeTransport, TransportRequest};
use crate::schema::FieldSpec;
use crate::value::ScalarKind;

struct Answers(Arc<Mutex<String>>, Arc<std::sync::atomic::AtomicUsize>);

#[async_trait::async_trait]
impl FakeTransport for Answers {
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> {
        self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(self.0.lock().unwrap().clone())
    }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

fn declaration() -> String {
    let gift = FieldSpec::variant("gift", "GiftAttachment", vec![FieldSpec::scalar("tier", ScalarKind::Str).nullable()]);
    let moment = FieldSpec { from: Some("MomentAttachment".into()), ..FieldSpec::variant("moment", "ProfileAttachmentMomentAttachment", vec![FieldSpec::scalar("momentId", ScalarKind::Id).nullable()]) };
    let mut root = FieldSpec::object("Profile");
    root.fields = vec![
        FieldSpec::scalar("id", ScalarKind::Id).nullable(),
        FieldSpec::scalar("balance", ScalarKind::Int).nullable(),
        FieldSpec::scalar("ratio", ScalarKind::Num).nullable(),
        FieldSpec::scalar("title", ScalarKind::Str).nullable(),
        FieldSpec::list("owners", ScalarKind::Id).nullable(),
        FieldSpec::union("attachment", "Attachment", vec![gift, moment]).nullable(),
    ];
    serde_json::to_string(&root).unwrap()
}

#[tokio::test]
async fn call_result_normalized() {
    let answer = Arc::new(Mutex::new(String::new()));
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let specs = json!({"models": [], "operations": [
        {"name": "Profile", "returns": serde_json::from_str::<Value>(&declaration()).unwrap()},
        {"name": "Raw", "returns": FieldSpec::raw("Raw")},
    ]});
    let database = Database::open(Config { path: None }, specs.to_string(), crate::ffi::OpenPorts { partial_operations: true, transport: Some(Arc::new(Answers(answer.clone(), sent.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    database.register_operations(json!([
        {"name": "Profile", "document": "query Profile { profile { id } }"},
        {"name": "Raw", "document": "query Raw { profile { id } }"},
        {"name": "Bare", "document": "query Bare { profile { id } }"},
    ]).to_string()).await.unwrap();
    database.set_online(true).await.unwrap();
    let call = |root: Value| {
        *answer.lock().unwrap() = json!({"data": {"profile": root}}).to_string();
        let database = database.clone();
        async move { database.call(database.session_epoch(), "Profile".into(), "{}".into()).await.map(|text| serde_json::from_str::<Value>(&text).unwrap()) }
    };
    let cases = [
        (json!({"id": 7, "balance": "12", "ratio": "1.5", "title": "Ada", "owners": [1, "2"], "extra": true}), json!({"id": "7", "balance": 12, "ratio": 1.5, "title": "Ada", "owners": ["1", "2"], "attachment": null})),
        (json!({"id": "7"}), json!({"id": "7", "balance": null, "ratio": null, "title": null, "owners": null, "attachment": null})),
        (json!({"attachment": {"__typename": "MomentAttachment", "momentId": 5, "tier": "gold"}}), json!({"id": null, "balance": null, "ratio": null, "title": null, "owners": null, "attachment": {"__typename": "MomentAttachment", "momentId": "5"}})),
        (json!({"attachment": {"__typename": "StickerAttachment", "id": 1}}), json!({"id": null, "balance": null, "ratio": null, "title": null, "owners": null, "attachment": {"__typename": "StickerAttachment"}})),
    ];
    for (root, normalized) in cases {
        assert_eq!(call(root.clone()).await.unwrap(), normalized, "{root}");
    }
    for refused in [json!({"title": 5}), json!({"balance": "many"}), json!({"attachment": {"tier": "gold"}})] {
        assert!(matches!(call(refused.clone()).await, Err(DbError::Invalid { .. })), "{refused}");
    }
    let before = sent.load(std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(database.call(database.session_epoch(), "Bare".into(), "{}".into()).await, Err(DbError::Invalid { .. })), "a call without a returns declaration is refused");
    assert_eq!(sent.load(std::sync::atomic::Ordering::SeqCst), before, "the refused call sent no request");
    *answer.lock().unwrap() = json!({"data": {"profile": {"id": 7, "extra": 1}}}).to_string();
    assert_eq!(database.call(database.session_epoch(), "Raw".into(), "{}".into()).await.unwrap(), r#"{"extra":1,"id":7}"#);
    database.session_reset().await.unwrap();
}

#[tokio::test]
async fn a_bare_list_of_models_is_refused_as_specs() {
    let refused = Database::open(Config { path: None }, "[]".into(), crate::ffi::OpenPorts::default()).await;
    assert!(matches!(refused, Err(DbError::Invalid { .. })), "{:?}", refused.as_ref().err());
}
