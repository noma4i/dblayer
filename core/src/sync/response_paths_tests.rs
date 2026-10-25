use super::*;
use super::rig::{params, put, remote, rig_on};
use crate::db::Db;
use crate::identity::RowId;
use crate::schema::{ActionDeclaration, Coverage, FieldSpec, ModelSpec, NodeRead, RelationSpec, RemoteShape, RootShape, VariableSource, NODE_PARAM};
use crate::transport::{EventSpec, MockTransport, Reply, Response, SubscriptionEvent};
use crate::upload::{UploadFile, UploadOperations};
use crate::value::ScalarKind;
use serde_json::json;
use std::io::Write;

#[tokio::test(start_paused = true)]
async fn server_entries_read_nested_paths_once_before_keyed_edits() {
    let mut failures = Vec::new();
    for entry in ["load", "node", "nodes", "event", "action", "lands", "upload"] {
        let mut reactions = FieldSpec::array("reactions").nullable().with_default(json!([]));
        reactions.app_type = Some("Reaction".into());
        reactions.fields = vec![FieldSpec::scalar("userId", ScalarKind::Id).read_from("user.id"), FieldSpec::scalar("body", ScalarKind::Str)];
        let mut model = ModelSpec::new("Message", vec![FieldSpec::scalar("id", ScalarKind::Id), reactions, FieldSpec::scalar("text", ScalarKind::Str).optional()]);
        for (name, operation, shape, node) in [
            ("feed", "Feed", RemoteShape::List { coverage: Coverage::Complete }, None),
            ("node", "Node", RemoteShape::Single, Some(NodeRead::One)),
            ("nodes", "Nodes", RemoteShape::List { coverage: Coverage::Complete }, Some(NodeRead::Many)),
        ] {
            let mut relation = RelationSpec::global(name);
            relation.remote = Some(remote(operation, shape, "5.minutes"));
            relation.node = node;
            if node == Some(NodeRead::One) {
                relation.is_global = false;
                relation.text_params.push(NODE_PARAM.into());
                relation.remote.as_mut().unwrap().variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
            }
            model.relations.push(relation);
        }
        for (name, mutation, root) in [("revise", "Change", "change.message"), ("photo", "Photo", "photo.message")] {
            let mut action = ActionDeclaration::new(name, RootShape::Upsert);
            action.mutation = Some(mutation.into());
            action.root = Some(root.into());
            action.upload = name == "photo";
            if entry == "lands" && name == "revise" {
                action.root = Some("change".into());
                action.lands.push(crate::schema::ActionLanding { model: "Message".into(), root: root.into() });
            }
            model.actions.push(action);
        }
        let rig = rig_on(Db::open_in_memory(vec![model], None).unwrap(), MockTransport::new(), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("Blob")), ..SyncConfig::default() });
        for (name, root) in [("Feed", "messages"), ("Node", "node"), ("Nodes", "nodes"), ("Changed", "changed"), ("Change", "change"), ("Photo", "photo"), ("Blob", "blob")] {
            rig.sync.register(OperationDef::new(name, "query x { x }", root));
        }
        let id = RowId::new("m1").unwrap();
        let value = json!({"id": "m1", "reactions": [{"user.id": "literal-must-not-win", "user": {"id": "u1"}, "body": "A"}]});
        match entry {
            "load" => {
                rig.transport.enqueue("Feed", Reply::data(json!({"messages": [value]})));
                rig.sync.load("Message", "feed", &params(json!({})), Default::default()).await.unwrap();
            }
            "node" => {
                rig.transport.enqueue("Node", Reply::data(json!({"node": value})));
                rig.sync.node("Message", "node", "m1", false).await.unwrap();
            }
            "nodes" => {
                rig.transport.enqueue("Nodes", Reply::data(json!({"nodes": [value]})));
                rig.sync.nodes("Message", "nodes", &["m1"]).await.unwrap();
            }
            "event" => {
                let _subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("changed"), "Changed", "Message", EventEffect::Upsert)).await.unwrap();
                rig.transport.emit("Changed", SubscriptionEvent::Next(Response::data(json!({"changed": value}))));
                rig.transport.await_consumed("Changed").await;
            }
            "action" | "lands" => {
                put(&rig, "Message", json!({"id": "m1", "reactions": []}));
                rig.transport.enqueue("Change", Reply::data(json!({"change": {"message": value}})));
                rig.db.act("Message", "revise", &id, params(json!({})), params(json!({}))).unwrap();
                rig.sync.flush().await.unwrap();
            }
            "upload" => {
                put(&rig, "Message", json!({"id": "m1", "reactions": []}));
                let mut file = tempfile::NamedTempFile::new().unwrap();
                file.write_all(b"photo").unwrap();
                rig.transport.enqueue("Blob", Reply::data(json!({"blob": {"url": "https://s3/x", "signedId": "s", "blobId": "b"}})));
                rig.transport.enqueue("Photo", Reply::data(json!({"photo": {"message": value}})));
                rig.sync.upload("Message", &id, "photo", UploadFile::new(file.path().to_str().unwrap())).await.unwrap();
                rig.sync.wait_uploads().await;
            }
            _ => unreachable!(),
        }
        let row = rig.db.find("Message", &id).unwrap().unwrap();
        if row.data["reactions"][0]["user.id"] != "u1" {
            failures.push(format!("{entry}: server key {:?}", row.data["reactions"][0]["user.id"]));
        }
        let patch = params(json!({"reactions": {"$keyed": {"by": "userId", "element": {"userId": "u1", "body": "C"}}}}));
        let edited = rig.db.act("Message", "edit", &id, patch, params(json!({}))).unwrap();
        if edited.row.data["reactions"] != json!([{"user.id": "u1", "body": "C"}]) {
            failures.push(format!("{entry}: keyed edit {:?}", edited.row.data["reactions"]));
        }
        rig.db.update("Message", &id, params(json!({"text": "kept"}))).unwrap();
        assert_eq!(rig.db.find("Message", &id).unwrap().unwrap().data["reactions"], edited.row.data["reactions"], "{entry}");
        let cleared = normalize::normalize_nodes(&rig.db, "Message", &[params(json!({"id": "m1", "reactions": null}))]).unwrap();
        assert_eq!(cleared.roots[0].get("reactions"), Some(&serde_json::Value::Null), "{entry}: explicit null");
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
