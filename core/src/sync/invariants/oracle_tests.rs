use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use super::super::rig::*;
use crate::identity::RowId;
use crate::transport::Reply;

#[derive(Default)]
struct Server {
    rows: BTreeMap<String, String>,
    created: usize,
}

struct Dice(u64);

impl Dice {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}

#[derive(Clone, Default)]
struct Record {
    id: Option<RowId>,
    shown: Option<String>,
    synced: Option<String>,
}

fn install_server(rig: &Rig, server: &Arc<Mutex<Server>>) {
    let server = server.clone();
    rig.transport.respond_with(move |request| {
        let mut server = server.lock().unwrap();
        let variables = &request.variables;
        let text = variables.get("text").and_then(Value::as_str).map(str::to_string);
        let id = variables.get("id").and_then(Value::as_str).map(str::to_string);
        match request.operation_name.as_str() {
            "CreateMoment" => {
                server.created += 1;
                let id = format!("srv{}", server.created);
                server.rows.insert(id.clone(), text.clone().unwrap());
                Some(Reply::data(json!({"createMoment": {"id": id, "text": text, "userId": null}})))
            }
            "UpdateMoment" => {
                let id = id.unwrap();
                server.rows.insert(id.clone(), text.clone().unwrap());
                Some(Reply::data(json!({"updateMoment": {"id": id, "text": text, "userId": null}})))
            }
            "DeleteMoment" => {
                let id = id.unwrap();
                server.rows.remove(&id);
                Some(Reply::data(json!({"deleteMoment": {"id": id}})))
            }
            _ => None,
        }
    });
}

fn texts(rig: &Rig) -> Vec<String> {
    let mut found: Vec<String> = rig.db.query(&crate::query::Query::new("moments")).unwrap().iter().filter_map(|row| row.text("text").map(str::to_string)).collect();
    found.sort();
    found
}

#[test]
fn inv_op_11_random_record_histories_end_equal_on_the_client_the_server_and_the_oracle() { crate::test_wait::paused(async {
    let mut kinds: BTreeMap<String, usize> = BTreeMap::new();
    for seed in 1..=120u64 {
        let rig = rig();
        let server = Arc::new(Mutex::new(Server::default()));
        install_server(&rig, &server);
        let mut records: Vec<Record> = Vec::new();
        for name in ["s1", "s2"] {
            put(&rig, "moments", moment(name, name));
            server.lock().unwrap().rows.insert(name.to_string(), name.to_string());
            records.push(Record { id: Some(rid_of(name)), shown: Some(name.to_string()), synced: Some(name.to_string()) });
        }
        let mut dice = Dice(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut history = Vec::new();
        for step in 0..14 {
            let index = dice.below(records.len() + 1);
            if index == records.len() {
                let text = format!("c{seed}-{step}");
                let row = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": text}))).unwrap();
                records.push(Record { id: Some(row.id.clone()), shown: Some(text.clone()), synced: None });
                history.push(format!("create {text}"));
                continue;
            }
            let action = dice.below(6);
            let record = &mut records[index];
            let (Some(id), Some(_)) = (record.id.clone(), record.shown.clone()) else { continue };
            match action {
                0 | 1 => {
                    let text = format!("u{seed}-{step}");
                    rig.db.update(&rig.db.admit().unwrap(), "moments", &id, params(json!({"text": text}))).unwrap();
                    record.shown = Some(text.clone());
                    history.push(format!("update {index} {text}"));
                }
                2 => {
                    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &id).unwrap();
                    record.shown = None;
                    history.push(format!("destroy {index}"));
                }
                3 => {
                    rig.db.discard(&rig.db.admit().unwrap(), "moments", &id, None).unwrap();
                    record.shown = record.synced.clone();
                    if record.synced.is_none() {
                        record.id = None;
                    }
                    history.push(format!("discard {index}"));
                }
                _ => {
                    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
                    for record in &mut records {
                        record.synced = record.shown.clone();
                    }
                    for record in records.iter_mut().filter(|record| record.shown.is_some()) {
                        let text = record.shown.clone().unwrap();
                        let row = rig.db.query(&crate::query::Query::new("moments")).unwrap().into_iter().find(|row| row.text("text") == Some(text.as_str()));
                        record.id = row.map(|row| row.id.clone());
                    }
                    history.push("flush".to_string());
                }
            }
            let mut expected: Vec<String> = records.iter().filter_map(|record| record.shown.clone()).collect();
            expected.sort();
            assert_eq!(texts(&rig), expected, "seed {seed} after {history:?}");
        }
        for entry in &history {
            *kinds.entry(entry.split(' ').next().unwrap().to_string()).or_default() += 1;
        }
        crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
        let mut expected: Vec<String> = records.iter().filter_map(|record| record.shown.clone()).collect();
        expected.sort();
        let mut on_server: Vec<String> = server.lock().unwrap().rows.values().cloned().collect();
        on_server.sort();
        assert_eq!(texts(&rig), expected, "seed {seed} client after {history:?}");
        assert_eq!(on_server, expected, "seed {seed} server after {history:?}");
        assert!(rig.db.outbox().unwrap().is_empty(), "seed {seed}: operations left after {history:?}");
        let temp_left = rig.db.query(&crate::query::Query::new("moments")).unwrap().iter().filter(|row| row.id.is_temp()).count();
        assert_eq!(temp_left, 0, "seed {seed}: temp rows left after {history:?}");
    }
    for kind in ["create", "update", "destroy", "discard", "flush"] {
        assert!(kinds.get(kind).copied().unwrap_or(0) >= 30, "too few {kind} steps: {kinds:?}");
    }
}) }

fn rid_of(text: &str) -> RowId {
    RowId::new(text).unwrap()
}
