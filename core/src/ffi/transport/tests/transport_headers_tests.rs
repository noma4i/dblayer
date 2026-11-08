use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::ForeignTransport;
use crate::ffi::cable::{CableEvents, CableOutcome, PlatformSocket};
use crate::ffi::{ByteProgress, Config, OpenOptions, OpenPorts, Transport};
use crate::test_wait::within;
use crate::transport::encrypted::tests::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::{HttpRequest, PutFile, RawOutcome, TransferIdentity, Upstream};

const GRAPHQL_HOST: &str = "https://graphql.test";
const URL: &str = "wss://test.invalid/cable";

struct NullSocket;

impl PlatformSocket for NullSocket {
    fn send(&self, _: String) -> bool { true }
    fn close(&self, _: u16, _: String) {}
    fn cancel(&self) {}
}

struct Headered {
    value: Mutex<String>,
    server: Arc<FakeServer>,
    sent: Mutex<Vec<(String, HashMap<String, String>)>>,
    cables: Mutex<Vec<HashMap<String, String>>>,
    puts: Mutex<Vec<HashMap<String, String>>>,
}

impl Headered {
    fn set(&self, value: &str) { *crate::lock::lock(&self.value) = value.into(); }

    fn generations(&self, host: &str) -> Vec<String> {
        crate::lock::lock(&self.sent).iter().filter(|(url, _)| url.starts_with(host)).map(|(_, headers)| headers["X-Gen"].clone()).collect()
    }

    fn cable_generations(&self) -> Vec<String> { crate::lock::lock(&self.cables).iter().map(|headers| headers["X-Gen"].clone()).collect() }
}

#[async_trait::async_trait]
impl Transport for Headered {
    fn app_headers(&self) -> HashMap<String, String> { HashMap::from([("X-Gen".into(), crate::lock::lock(&self.value).clone())]) }
    async fn send(&self, request: HttpRequest) -> RawOutcome {
        crate::lock::lock(&self.sent).push((request.url.clone(), request.headers.clone()));
        if request.url.starts_with(GRAPHQL_HOST) { return RawOutcome::response(200, r#"{"data":{"x":1}}"#); }
        let path = request.url.strip_prefix(crate::transport::encrypted::tests::vectors_tests::origin().trim_end_matches('/')).expect("an encrypted request goes to the origin");
        match crate::transport::HttpExchange::post(self.server.as_ref(), path, request.body, request.purpose, crate::transport::WireIds::bootstrap()).await {
            Ok(reply) => RawOutcome::response(reply.status, reply.body),
            Err(error) => RawOutcome::failed(crate::transport::RawFailureKind::Io, error.to_string()),
        }
    }
    async fn open_cable(&self, _: String, headers: HashMap<String, String>, _: u64, _: Arc<dyn CableEvents>) -> CableOutcome {
        crate::lock::lock(&self.cables).push(headers);
        CableOutcome::Open { connection: Arc::new(NullSocket) }
    }
    async fn put(&self, target: crate::ffi::PutTarget, _: PutFile, _: TransferIdentity, _: Arc<ByteProgress>) -> RawOutcome {
        crate::lock::lock(&self.puts).push(target.headers);
        RawOutcome::response(200, "")
    }
}

/// P-LIFE-17. Observable: the headers of each request the platform port receives. A value the app changes between two requests is in the next one on the query, the mutation, the cable and the encrypted HTTP. The PUT to the presigned URL carries the presign headers and none of the app.
#[tokio::test(flavor = "multi_thread")]
async fn life_prop_headers_per_request() {
    let port = Arc::new(Headered { value: Mutex::new("first".into()), server: FakeServer::new(Arc::new(crate::runtime::SystemClock)), sent: Mutex::default(), cables: Mutex::default(), puts: Mutex::default() });
    let transport = ForeignTransport::plain(port.clone(), Some(format!("{GRAPHQL_HOST}/graphql"))).unwrap();
    let request = || crate::transport::GraphQLRequest::new("query Q { x }", "Q", serde_json::json!({})).by(crate::transport::WireIds::bootstrap());
    let ports = OpenPorts { executor: Some(port.clone()), encrypted: Some(crate::transport::encrypted::tests::protocol_tests::config()), credentials: Some(Arc::new(crate::ffi::tests::encrypted_tests::Held(Arc::new(MemoryCredentials::default())))), ..Default::default() };
    let database = within(crate::ffi::OpenDatabase::new().open(Config { path: None, data_version: None }, crate::ffi::EMPTY_SPECS.into(), OpenOptions::default(), ports, None)).await.unwrap();
    database.set_online(true).unwrap();
    let origin = crate::transport::encrypted::tests::vectors_tests::origin().trim_end_matches('/').to_string();
    for value in ["first", "second"] {
        port.set(value);
        let before = (port.generations(GRAPHQL_HOST).len(), port.generations(&origin).len(), port.cable_generations().len());
        transport.query(request()).await.unwrap();
        transport.mutate(request()).await.unwrap();
        drop(transport.open_cable(URL).await.unwrap());
        within(database.session_call("otp".into(), r#"{"phone":"+61400000000"}"#.into())).await.unwrap();
        let (graphql, encrypted, cables) = (port.generations(GRAPHQL_HOST), port.generations(&origin), port.cable_generations());
        assert_eq!(graphql[before.0..], [value, value], "{value}: the query and the mutation read the headers again");
        assert!(encrypted.len() > before.1 && encrypted[before.1..].iter().all(|seen| seen == value), "{value}: the encrypted request carries the current value: {encrypted:?}");
        assert_eq!(cables[before.2..], [value], "{value}: the cable reads the headers again");
    }
    let put = crate::transport::PutRequest {
        url: "https://files.test/blob".into(),
        headers: vec![("x-amz-meta".into(), "presign".into())],
        file: PutFile { path: "/none".into(), offset: 0, length: 0 },
        identity: TransferIdentity { operation_id: 1, item_id: "a".into(), step: crate::transport::TransferStep::Put },
        cancel: crate::transport::CancelToken::new(),
        sent: Arc::new(|_| {}),
    };
    transport.put(put).await.unwrap();
    let puts = crate::lock::lock(&port.puts).clone();
    assert_eq!(puts.len(), 1);
    assert_eq!(puts[0].get("x-amz-meta").map(String::as_str), Some("presign"), "the presign header is on the PUT");
    assert!(!puts[0].contains_key("X-Gen"), "the app headers are not on the PUT: {:?}", puts[0]);
    within(database.close_database()).await.unwrap();
}
