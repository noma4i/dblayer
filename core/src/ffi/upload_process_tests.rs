use super::*;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use serde_json::{json, Value};
use super::sealed_errors_tests::moment_spec;
use super::upload_resume_tests::Bytes;
use crate::transport::encrypted::fake_server_tests::FakeServer;
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::HttpExchange;
use crate::upload::{UploadFile, UploadOperations};
use crate::test_wait::join_within;

struct Remote(String);

impl Remote {
    fn call(&self, body: Value) -> Result<Value, DbError> {
        let call = || -> std::io::Result<Value> {
            let mut stream = TcpStream::connect(&self.0)?;
            stream.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
            stream.set_write_timeout(Some(std::time::Duration::from_secs(10)))?;
            writeln!(stream, "{body}")?;
            let mut line = String::new();
            BufReader::new(stream).read_line(&mut line)?;
            Ok(serde_json::from_str(&line)?)
        };
        call().map_err(|error| DbError::Network { detail: error.to_string() })
    }
}

#[async_trait::async_trait]
impl EncryptedHttp for Remote {
    async fn post(&self, path: String, body: String, _: bool) -> Result<EncryptedReply, DbError> {
        let reply = self.call(json!({"post": path, "body": body}))?;
        Ok(EncryptedReply { status: reply["status"].as_u64().unwrap() as u16, body: reply["body"].as_str().unwrap().into() })
    }
}

impl CredentialStore for Remote {
    fn get(&self) -> Result<Option<String>, DbError> { Ok(self.call(json!({"get": true}))?.as_str().map(str::to_string)) }
    fn set(&self, credential: String) -> Result<(), DbError> { self.call(json!({"set": credential})).map(|_| ()) }
    fn clear(&self) -> Result<(), DbError> { self.call(json!({"clear": true})).map(|_| ()) }
}

async fn child(mode: &str) {
    let path = std::env::var("DBLAYER_UPLOAD_DB").unwrap();
    if mode == "resume" {
        std::fs::remove_file(format!("{path}.mp4")).unwrap();
    }
    let remote = Arc::new(Remote(std::env::var("DBLAYER_UPLOAD_PEER").unwrap()));
    let database = Database::open(Config { path: Some(path.clone()), data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![moment_spec()]).unwrap(), Some(Arc::new(Bytes)), Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::protocol_tests::config() }), Some(remote.clone()), Some(remote.clone()), Some(UploadOperations::empty().create_blob("CreateBlob")), None).await.unwrap();
    database.register_operations(json!([
        {"name": "CreateBlob", "document": "mutation CreateBlob { createBlob { id } }", "rootPath": "createBlob"},
        {"name": "MakeMoment", "document": "mutation MakeMoment { makeMoment { id } }", "rootPath": "makeMoment"}
    ]).to_string()).unwrap();
    if mode == "seed" {
        database.session_call("social".into(), super::session_guards_tests::SOCIAL_BODY.into()).await.unwrap();
        remote.call(json!({"refuse": true})).unwrap();
        let source = format!("{path}.mp4");
        std::fs::write(&source, b"abc").unwrap();
        let input = UploadFile::new(&source);
        database.upload(database.session_epoch(), "Moment".into(), "42".into(), "upload".into(), crate::ffi::upload_wire::WireUploadRequest::of(&input)).await.unwrap();
        database.sync.as_ref().unwrap().wait_uploads().await;
        assert_eq!(database.db.outbox().unwrap().len(), 1);
    } else {
        database.session_resume().await.unwrap();
        database.retry("Moment".into(), "42".into(), None).await.unwrap();
        database.sync.as_ref().unwrap().wait_uploads().await;
        assert!(database.db.outbox().unwrap().is_empty());
    }
}

#[test]
fn upload_resume_survives_the_exit_of_the_sending_process() {
    if let Ok(mode) = std::env::var("DBLAYER_UPLOAD_CHILD") {
        tokio::runtime::Runtime::new().unwrap().block_on(child(&mode));
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = FakeServer::new(Arc::new(crate::pacing::SystemClock));
    server.register("mutation CreateBlob { createBlob { id } }");
    server.register("mutation MakeMoment { makeMoment { id } }");
    server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"}, "makeMoment": {"id": "42"}}));
    let peer = server.clone();
    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let credential: Mutex<Option<String>> = Mutex::new(None);
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut line = String::new();
            BufReader::new(&stream).read_line(&mut line).unwrap();
            let call: Value = serde_json::from_str(&line).unwrap();
            let reply = if let Some(path) = call["post"].as_str() {
                let reply = runtime.block_on(peer.post(path, call["body"].as_str().unwrap().into(), false)).unwrap();
                json!({"status": reply.status, "body": reply.body})
            } else if call["get"] == true {
                json!(*credential.lock().unwrap())
            } else if let Some(value) = call["set"].as_str() {
                *credential.lock().unwrap() = Some(value.into());
                Value::Null
            } else {
                if call["refuse"] == true { peer.refuse_next("ACCOUNT_UNAVAILABLE", false); }
                if call["clear"] == true { *credential.lock().unwrap() = None; }
                Value::Null
            };
            writeln!(stream, "{reply}").unwrap();
            if call["stop"] == true { break; }
        }
    });
    let results: Vec<_> = ["seed", "resume"].into_iter().map(|mode| {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "ffi::upload_process_tests::upload_resume_survives_the_exit_of_the_sending_process", "--nocapture"])
            .env("DBLAYER_UPLOAD_CHILD", mode).env("DBLAYER_UPLOAD_DB", directory.path().join("db.sqlite")).env("DBLAYER_UPLOAD_PEER", &address)
            .output().unwrap()
    }).collect();
    Remote(address).call(json!({"stop": true})).unwrap();
    join_within(thread);
    for result in results { assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stdout)); }
    assert_eq!(server.executed().iter().filter(|body| body["operationName"] == "CreateBlob").count(), 1);
}
