use crate::ffi::{FakeTransport};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::{Config, Database, DbError, Transport};
use crate::schema::{Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RemoteSpec};
use crate::value::ScalarKind;

pub(super) struct Refusing {
    pub(super) calls: AtomicUsize,
    pub(super) variables: std::sync::Mutex<Vec<String>>,
    answer: Result<&'static str, fn() -> DbError>,
}

#[async_trait::async_trait]
impl FakeTransport for Refusing {
    async fn query(&self, request: super::TransportRequest) -> Result<String, DbError> {
        self.variables.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(request.variables_json);
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.answer.map(str::to_string).map_err(|make| make())
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

pub(super) async fn rig(answer: Result<&'static str, fn() -> DbError>) -> (Arc<Database>, Arc<Refusing>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let mut relation = RelationSpec::global("feed");
    relation.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: "5.minutes".into(),
        empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(relation);
    let transport = Arc::new(Refusing { calls: AtomicUsize::new(0), variables: Default::default(), answer });
    let models = serde_json::to_string(&vec![model]).unwrap();
    let db = Database::open(Config { path: None, data_version: None }, models, crate::ffi::OpenPorts { transport: Some(transport.clone() as Arc<dyn Transport>), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id } }","rootPath":"moments"}]"#.into()).await.unwrap();
    db.set_online(true).unwrap();
    (db, transport)
}

fn unauthorized() -> DbError {
    DbError::Http { status: 401, detail: "unauthorized".into() }
}

const REFUSED: &str = r#"{"errors":[{"message":"nope"},{"message":"bad"}]}"#;

#[tokio::test]
async fn inv_api_03_a_refused_call_keeps_its_kind_and_details_and_is_sent_once() {
    for answer in [Err(unauthorized as fn() -> DbError), Ok(REFUSED)] {
        for call in ["run", "refresh"] {
            let (db, transport) = rig(answer).await;
            let error = match call {
                "run" => db.run(db.session_epoch(), "Feed".into(), "{}".into()).await.unwrap_err(),
                _ => db.refresh(db.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into()).await.unwrap_err(),
            };
            match answer {
                Err(_) => assert!(matches!(&error, DbError::Http { status: 401, detail } if detail.contains("unauthorized")), "{call}: {error:?}"),
                Ok(_) => assert!(matches!(&error, DbError::Graphql { messages, .. } if messages == &["nope", "bad"]), "{call}: {error:?}"),
            }
            assert_eq!(transport.calls.load(Ordering::SeqCst), 1, "{call}");
        }
    }
}
