use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};

use super::crash_image_call_tests::copy_image;
use super::crash_image_support::{advancing, advancing_by, over_images, wal_length, Capture};
use super::rig_tests::bound;
use super::upload_stand::{clip, plain};
use super::upload_tests::Puts;
use crate::ffi::tests::encrypted_tests::ThroughServer;
use crate::ffi::tests::sealed_errors_tests::moment_spec;
use crate::ffi::tests::session_guards_tests::{WatchedHeld, SOCIAL_BODY};
use crate::ffi::{Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedReply, FakeHttp, OpenPorts, TestClock};
use crate::transport::encrypted::tests::fake_server_tests::FakeServer;
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::NetworkPurpose;

const MUTATIONS: [(&str, &str); 7] = [("CreateBlob", "createBlob"), ("MakeMoment", "makeMoment"), ("InitMultipart", "initMultipart"), ("PresignParts", "presignParts"), ("CompleteMultipart", "completeMultipart"), ("AbortMultipart", "abortMultipart"), ("Ping", "ping")];
const READ: &str = "query Pong { ping { id } }";

fn mutation(name: &str, root: &str) -> String { format!("mutation {name} {{ {root} {{ id }} }}") }

/// The server double of the stand: one sequence for every request of the session, the documents the app declared, and the answers the upload steps need.
fn server(clock: &TestClock) -> Arc<FakeServer> {
    let server = FakeServer::new(clock.manual());
    for (name, root) in MUTATIONS { server.register(&mutation(name, root)); }
    server.register(READ);
    server.reply_with(json!({
        "createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"},
        "initMultipart": {"multipartUpload": {"key": "blob-key", "uploadId": "multi"}},
        "presignParts": {"parts": [{"partNumber": 1, "url": "https://p1"}]},
        "completeMultipart": {"signedId": "multi-sid", "blobId": "bid"},
        "abortMultipart": {"ok": true},
        "makeMoment": {"id": "42", "text": "clip"},
        "ping": {"id": "p"},
    }));
    server
}

struct Wire {
    path: String,
    body: String,
    log_length: u64,
}

/// The HTTP the client talks through: it records every request with the length of the log of the live database at that moment, and refuses a logout when the scenario says so.
struct Recording {
    server: Arc<FakeServer>,
    live: Option<PathBuf>,
    wires: Mutex<Vec<Wire>>,
    refuse_logout: bool,
}

#[async_trait]
impl FakeHttp for Recording {
    async fn post(&self, path: String, body: String, purpose: NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let log_length = self.live.as_deref().map_or(0, wal_length);
        self.wires.lock().unwrap().push(Wire { path: path.clone(), body: body.clone(), log_length });
        if self.refuse_logout && path.ends_with("logout") { self.server.refuse_next("BLOCKED", true); }
        FakeHttp::post(&ThroughServer(self.server.clone()), path, body, purpose).await
    }
}

async fn open(path: &Path, http: Arc<Recording>, credentials: Arc<WatchedHeld>, clock: Arc<TestClock>) -> Arc<Database> {
    let restored = credentials.inner.value().is_some();
    let ports = OpenPorts { partial_operations: true, transport: Some(Arc::new(Puts::default())), encrypted: Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::tests::protocol_tests::config() }), credentials: Some(credentials), http: Some(http), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
    let config = Config { path: Some(path.to_string_lossy().into_owned()), data_version: None };
    let database = Database::open_with_clock(config, crate::ffi::specs(serde_json::to_string(&vec![moment_spec()]).unwrap()), ports, clock).await.unwrap();
    let mut declared: Vec<Value> = MUTATIONS.iter().map(|(name, root)| json!({"name": name, "document": mutation(name, root), "rootPath": root})).collect();
    declared.push(json!({"name": "Pong", "document": READ, "rootPath": "read"}));
    database.register_operations(Value::Array(declared).to_string()).await.unwrap();
    database.set_online(true).await.unwrap();
    if !restored { bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap(); }
    database
}

async fn quiet(database: &Database, clock: &TestClock) {
    advancing(clock, async {
        bound(crate::ffi::settled(database)).await.unwrap();
    }).await
}

#[derive(Clone, Copy, Debug)]
enum Exchange {
    SessionCall,
    Call,
    Read,
    Create,
    Upload,
    TwoMutations,
    LogoutRefused,
    CallAnswerLost,
    CallsInARow,
    CallBesideMutation,
    CallCancelled,
    CallExpired,
}

struct Live {
    database: Arc<Database>,
    server: Arc<FakeServer>,
    clock: Arc<TestClock>,
    directory: PathBuf,
}

async fn call(database: Arc<Database>, epoch: u64, operation: &str) -> Result<String, DbError> { database.run(epoch, operation.into(), "{}".into()).await }

async fn mutate(database: &Database, text: &str) {
    let epoch = database.session_epoch();
    let receipt = bound(database.send(plain("Moment", &json!({"text": text}).to_string(), epoch))).await.unwrap();
    bound(database.confirmed(receipt, false)).await.unwrap();
}

async fn drive(exchange: Exchange, live: &Live) {
    let (database, clock, server) = (&live.database, &live.clock, &live.server);
    let epoch = database.session_epoch();
    match exchange {
        Exchange::SessionCall => { bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap(); }
        Exchange::Call => { bound(call(database.clone(), epoch, "Ping")).await.unwrap(); }
        Exchange::Read => { bound(call(database.clone(), epoch, "Pong")).await.unwrap(); }
        Exchange::Create => mutate(database, "a").await,
        Exchange::Upload => {
            let source = live.directory.join("clip.mp4");
            std::fs::write(&source, b"abc").unwrap();
            let receipt = bound(database.send(clip("Moment", r#"{"text":"clip"}"#, "upload", &source, epoch))).await.unwrap();
            bound(database.confirmed(receipt, false)).await.unwrap();
        }
        Exchange::TwoMutations => {
            let first = bound(database.send(plain("Moment", r#"{"text":"a"}"#, epoch))).await.unwrap();
            let second = bound(database.send(plain("Moment", r#"{"text":"b"}"#, epoch))).await.unwrap();
            bound(database.confirmed(first, false)).await.unwrap();
            bound(database.confirmed(second, false)).await.unwrap();
        }
        Exchange::LogoutRefused => { let _ = bound(database.session_call("logout".into(), "{}".into())).await; }
        Exchange::CallAnswerLost => {
            server.lose_next_answer();
            let _ = advancing_by(clock, 1_000, bound(call(database.clone(), epoch, "Ping"))).await;
            quiet(database, clock).await;
        }
        Exchange::CallsInARow => {
            server.lose_next_answer();
            for operation in ["Ping", "Ping", "Ping"] { let _ = advancing_by(clock, 1_000, bound(call(database.clone(), epoch, operation))).await; }
            quiet(database, clock).await;
        }
        Exchange::CallBesideMutation => {
            server.lose_next_answer();
            let _ = advancing_by(clock, 1_000, bound(call(database.clone(), epoch, "Ping"))).await;
            advancing_by(clock, 1_000, mutate(database, "a")).await;
            quiet(database, clock).await;
        }
        Exchange::CallCancelled => {
            server.lose_next_answer();
            let armed = clock.armed();
            let task = tokio::spawn(call(database.clone(), epoch, "Ping"));
            bound(clock.await_armed(armed + 1)).await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            quiet(database, clock).await;
        }
        Exchange::CallExpired => {
            server.lose_next_answer();
            let mut armed = clock.armed();
            let mut task = tokio::spawn(call(database.clone(), epoch, "Ping"));
            let outcome = loop {
                tokio::select! {
                    result = &mut task => break result.unwrap(),
                    () = clock.await_armed(armed) => { armed = clock.armed(); server.lose_next_answer(); clock.advance(10_000); }
                }
            };
            assert!(outcome.is_err(), "the call ends with an error at its deadline");
            quiet(database, clock).await;
        }
    }
}

/// The server as it stands for the image: the requests the live client sent up to its commit, replayed on a fresh double. A request sent after the commit is in too when `accepted`: the client died before it wrote the answer.
async fn replay(wires: &[Wire], cuts: &[usize], kept: usize, image: usize, accepted: bool, clock: &TestClock, refuse_logout: bool) -> Arc<FakeServer> {
    let server = server(clock);
    let http = Recording { server: server.clone(), live: None, wires: Mutex::default(), refuse_logout };
    for (index, wire) in wires.iter().enumerate() {
        if index < kept || cuts[index] < image || (accepted && cuts[index] == image) {
            let _ = FakeHttp::post(&http, wire.path.clone(), wire.body.clone(), NetworkPurpose::Standard).await;
        }
    }
    server
}

/// A process killed at any commit of the exchange leaves a file that, opened against the server that outlives the client (the state at that commit, and the state after it accepted the request the client sent next), settles, takes a new mutation without `SEQUENCE_CONFLICT`, spends no sequence under a new request id, ends on the sequence the server holds, and has every request executed once.
async fn at_every_commit(exchange: Exchange) {
    let clock = TestClock::new(1_000_000);
    let server = server(&clock);
    let directory = tempfile::tempdir().unwrap();
    let live_file = directory.path().join("live.sqlite");
    let refuse_logout = matches!(exchange, Exchange::LogoutRefused);
    let recording = Arc::new(Recording { server: server.clone(), live: Some(live_file.clone()), wires: Mutex::default(), refuse_logout });
    let credentials = Arc::new(WatchedHeld::default());
    let database = open(&live_file, recording.clone(), credentials.clone(), clock.clone()).await;
    quiet(&database, &clock).await;
    rusqlite::Connection::open(&live_file).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let kept = recording.wires.lock().unwrap().len();
    let base = Capture::begin(&live_file);
    let live = Live { database: database.clone(), server: server.clone(), clock: clock.clone(), directory: directory.path().to_path_buf() };
    drive(exchange, &live).await;
    quiet(&database, &clock).await;
    let (capture, files) = base.end(&live_file);
    let secret = credentials.inner.value().unwrap();
    let wires: Arc<Vec<Wire>> = Arc::new(std::mem::take(&mut *recording.wires.lock().unwrap()));
    let cuts: Arc<Vec<usize>> = Arc::new(wires.iter().map(|wire| capture.cut_at(wire.log_length)).collect());
    assert!(wires.len() > kept, "{exchange:?}: the scenario sent a request: {} of {kept}", wires.len());

    over_images(&capture, &files, |index, image| {
        let (secret, wires, cuts) = (secret.clone(), wires.clone(), cuts.clone());
        async move {
            let directory = tempfile::tempdir().unwrap();
            for accepted in [false, true] {
                let copy = directory.path().join(format!("copy-{accepted}.sqlite"));
                copy_image(&image, &copy);
                let clock = TestClock::new(1_060_000);
                let server = replay(&wires, &cuts, kept, index, accepted, &clock, refuse_logout).await;
                let http = Arc::new(Recording { server: server.clone(), live: None, wires: Mutex::default(), refuse_logout });
                let credentials = Arc::new(WatchedHeld::default());
                CredentialStore::set(credentials.as_ref(), secret.clone()).unwrap();
                let database = open(&copy, http, credentials, clock.clone()).await;
                quiet(&database, &clock).await;
                let epoch = database.session_epoch();
                let receipt = bound(database.send(plain("Moment", r#"{"text":"next"}"#, epoch))).await.unwrap();
                advancing(&clock, bound(database.confirmed(receipt, false))).await.unwrap_or_else(|error| panic!("{exchange:?} cut {index} accepted={accepted}: the new mutation: {error:?}"));
                quiet(&database, &clock).await;
                bound(database.close_database()).await.unwrap();

                let seen = server.seen();
                let mut pairs: BTreeMap<i64, BTreeSet<String>> = BTreeMap::new();
                for request in &seen {
                    if let Some(sequence) = request["sequence"].as_i64() { pairs.entry(sequence).or_default().insert(request["requestId"].as_str().unwrap().to_string()); }
                }
                for (sequence, ids) in &pairs { assert_eq!(ids.len(), 1, "{exchange:?} cut {index} accepted={accepted}: sequence {sequence} went with {ids:?}"); }
                let held: i64 = rusqlite::Connection::open(&copy).unwrap().query_row("SELECT sequence FROM encrypted_session", [], |row| row.get(0)).unwrap();
                assert_eq!(pairs.keys().max().copied().unwrap_or(0), held, "{exchange:?} cut {index} accepted={accepted}: the server and the saved session agree on the sequence");
                let refused = usize::from(refuse_logout && seen.iter().any(|request| request["path"].as_str().is_some_and(|path| path.ends_with("logout"))));
                assert_eq!(server.executed().len() + refused, pairs.len(), "{exchange:?} cut {index} accepted={accepted}: every sequence executed once");
            }
        }
    }).await;
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_a_session_call_opens_on_the_living_server() { at_every_commit(Exchange::SessionCall).await; }
#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_an_interactive_call_opens_on_the_living_server() { at_every_commit(Exchange::Call).await; }
#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_a_read_opens_on_the_living_server() { at_every_commit(Exchange::Read).await; }
#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_a_queued_mutation_opens_on_the_living_server() { at_every_commit(Exchange::Create).await; }
#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_an_upload_opens_on_the_living_server() { at_every_commit(Exchange::Upload).await; }
#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_two_mutations_in_a_row_opens_on_the_living_server() { at_every_commit(Exchange::TwoMutations).await; }
#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_a_refused_logout_opens_on_the_living_server() { at_every_commit(Exchange::LogoutRefused).await; }
#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_an_interactive_call_with_a_lost_answer_opens_on_the_living_server() { at_every_commit(Exchange::CallAnswerLost).await; }
#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_interactive_calls_in_a_row_opens_on_the_living_server() { at_every_commit(Exchange::CallsInARow).await; }
#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_an_interactive_call_beside_a_mutation_opens_on_the_living_server() { at_every_commit(Exchange::CallBesideMutation).await; }
#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_a_cancelled_interactive_call_opens_on_the_living_server() { at_every_commit(Exchange::CallCancelled).await; }
#[tokio::test(flavor = "multi_thread")]
async fn an_image_after_an_expired_interactive_call_opens_on_the_living_server() { at_every_commit(Exchange::CallExpired).await; }

/// Requirement: an interactive call and a queued mutation started together both end, at either order of the inputs, and the server executes the request of each once. Observable: the outcome of the call, the confirmation of the mutation, the executed requests of the server.
#[test]
fn an_interactive_call_beside_a_queued_mutation_both_end() {
    crate::test_wait::in_own_process(module_path!(), "an_interactive_call_beside_a_queued_mutation_both_end", &["call_first", "mutation_first"], |order| {
        tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
            let clock = TestClock::new(1_000_000);
            let server = server(&clock);
            let directory = tempfile::tempdir().unwrap();
            let live_file = directory.path().join("live.sqlite");
            let recording = Arc::new(Recording { server: server.clone(), live: None, wires: Mutex::default(), refuse_logout: false });
            let database = open(&live_file, recording, Arc::new(WatchedHeld::default()), clock.clone()).await;
            quiet(&database, &clock).await;
            let before = server.executed().len();
            let epoch = database.session_epoch();
            server.lose_next_answer();
            let start_call = || tokio::spawn(call(database.clone(), epoch, "Ping"));
            let start_mutation = || { let database = database.clone(); tokio::spawn(async move { mutate(&database, "a").await }) };
            let (called, mutated) = if order == "call_first" { let called = start_call(); (called, start_mutation()) } else { let mutated = start_mutation(); (start_call(), mutated) };
            advancing_by(&clock, 1_000, async {
                bound(mutated).await.unwrap();
                bound(called).await.unwrap().unwrap();
            }).await;
            quiet(&database, &clock).await;
            assert_eq!(server.executed().len() - before, 2, "{order}: the request of the call and the request of the mutation executed once each");
            database.close_now();
        });
    });
}
