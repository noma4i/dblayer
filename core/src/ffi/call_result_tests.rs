use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use super::{Config, Database, DbError, FakeTransport, TransportRequest};
use crate::schema::FieldSpec;
use crate::value::ScalarKind;

struct Answers(Arc<Mutex<String>>);

#[async_trait::async_trait]
impl FakeTransport for Answers {
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { Ok(self.0.lock().unwrap().clone()) }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

fn declaration() -> String {
    let gift = FieldSpec::variant("gift", "GiftAttachment", vec![FieldSpec::scalar("tier", ScalarKind::Str).nullable()]);
    let moment = FieldSpec::variant("moment", "MomentAttachment", vec![FieldSpec::scalar("momentId", ScalarKind::Id).nullable()]);
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
    let database = Database::open(Config { path: None, data_version: None }, "[]".into(), crate::ffi::OpenPorts { transport: Some(Arc::new(Answers(answer.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    database.register_operations(r#"[{"name":"Profile","document":"query Profile { profile { id } }"}]"#.into()).await.unwrap();
    database.set_online(true).unwrap();
    let declared = declaration();
    let call = |root: Value| {
        *answer.lock().unwrap() = json!({"data": {"profile": root}}).to_string();
        let (database, declared) = (database.clone(), declared.clone());
        async move { database.call(database.session_epoch(), "Profile".into(), "{}".into(), declared).await.map(|text| serde_json::from_str::<Value>(&text).unwrap()) }
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
    database.session_reset().await.unwrap();
}
