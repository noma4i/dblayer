use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

use serde_json::Value;

use crate::ddl::table_name;
use crate::db::{Db, DbError};
use crate::operation::{open_operations, Operation};
use crate::syncstate::{fold_entries, operation_keys, read_sync, SyncEntry, SyncInfo, SyncKey};

use super::findings::Findings;
use super::observers::Universe;

const CACHE_ATTEMPTS: usize = 4;
const MODELS: [&str; 2] = ["moments", "users"];

fn entry_of(connection: &rusqlite::Connection, operation: &Operation) -> rusqlite::Result<SyncEntry> {
    let (_, action) = Operation::split_action_key(&operation.action_key).unwrap_or_default();
    let fields = if action == "update" {
        let bytes: Vec<u8> = connection.query_row("SELECT input FROM op WHERE seq = ?1", [operation.id.0], |row| row.get(0))?;
        match serde_json::from_slice::<Value>(&bytes) {
            Ok(Value::Object(input)) => input.keys().filter(|name| *name != "id").cloned().collect(),
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };
    Ok(SyncEntry {
        action,
        status: operation.status,
        transport_started: operation.transport_started,
        attempts: operation.attempts,
        error: operation.error.clone(),
        since: operation.since,
        fields,
        upload: operation.upload.clone(),
    })
}

fn describe(info: Option<&SyncInfo>) -> String {
    info.map_or("none".to_string(), |info| format!("{:?} {:?} attempts {}", info.state, info.actions, info.attempts))
}

pub fn queue(db: &Db, findings: &Findings) {
    let outcome = db.shared.read(|reader| {
        let connection = reader.connection();
        let operations = open_operations(connection)?;
        let mut expected: BTreeMap<SyncKey, Vec<SyncEntry>> = BTreeMap::new();
        let mut seen_seq = HashSet::new();
        let mut problems = Vec::new();
        for operation in &operations {
            if !seen_seq.insert(operation.id.0) {
                problems.push(format!("operation {} listed twice", operation.id.0));
            }
            let entry = entry_of(connection, operation)?;
            for key in operation_keys(&operation.action_key, &operation.row_ids, operation.temp_id.as_ref()) {
                expected.entry(key).or_default().push(entry.clone());
            }
        }
        let mut statement = connection.prepare("SELECT model, row_id FROM row_sync")?;
        let stored: BTreeSet<SyncKey> = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?;
        for key in stored.iter().filter(|key| !expected.contains_key(*key)) {
            problems.push(format!("row_sync keeps {}/{} with no open operation", key.0, key.1));
        }
        for (key, entries) in &expected {
            let want = fold_entries(entries);
            let have = read_sync(connection, &key.0, std::slice::from_ref(&key.1))?.remove(&key.1);
            if want != have {
                problems.push(format!("row_sync of {}/{} is {} but the open operations give {}", key.0, key.1, describe(have.as_ref()), describe(want.as_ref())));
            }
        }
        let temp_ids: HashSet<String> = operations.iter().filter_map(|operation| operation.temp_id.as_ref().map(ToString::to_string)).collect();
        let referenced: HashSet<String> = operations.iter().flat_map(|operation| operation.row_ids.iter().map(ToString::to_string)).collect();
        let mut rows = connection.prepare(&format!("SELECT id FROM {} WHERE id LIKE 'temp-%'", table_name("moments")))?;
        let present: Vec<String> = rows.query_map([], |row| row.get(0))?.collect::<Result<_, _>>()?;
        for id in &present {
            if !temp_ids.contains(id) && !referenced.contains(id) {
                problems.push(format!("temp row {id} has no open operation"));
            }
        }
        for id in referenced.iter().filter(|id| id.starts_with("temp-") && !temp_ids.contains(*id)) {
            problems.push(format!("an operation still points at temp id {id} but no open create owns it"));
        }
        Ok(problems)
    });
    match outcome {
        Ok(problems) => problems.iter().for_each(|problem| findings.report("queue", problem)),
        Err(error) => findings.report("queue", format!("check failed: {error}")),
    }
}

pub fn lists(db: &Db, findings: &Findings) {
    let outcome: Result<Vec<String>, DbError> = db.shared.read(|reader| {
        let connection = reader.connection();
        let mut problems = Vec::new();
        let mut keys = connection.prepare("SELECT DISTINCT scope_key FROM scope_member WHERE model = 'moments'")?;
        let scopes: Vec<String> = keys.query_map([], |row| row.get(0))?.collect::<Result<_, _>>()?;
        for scope in scopes {
            let members = reader.scope_members("moments", &scope)?;
            let ids: Vec<String> = members.iter().map(|(id, _)| id.clone()).collect();
            let unique: HashSet<&String> = ids.iter().collect();
            if unique.len() != ids.len() {
                problems.push(format!("scope {scope} lists a row twice"));
            }
            let stored = reader.stored_rows("moments", &ids)?;
            for id in ids.iter().filter(|id| stored.get(*id).and_then(|record| record.row.as_ref()).is_none()) {
                problems.push(format!("scope {scope} holds row {id} that is not stored"));
            }
        }
        let mut windows = connection.prepare("SELECT key, ids FROM query_window")?;
        let rows: Vec<(String, Vec<u8>)> = windows.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?;
        for (key, bytes) in rows {
            let ids: Vec<Value> = serde_json::from_slice(&bytes).unwrap_or_default();
            let texts: Vec<String> = ids.iter().filter_map(|id| id.as_str().map(str::to_string)).collect();
            let unique: HashSet<&String> = texts.iter().collect();
            if unique.len() != texts.len() {
                problems.push(format!("window {key} lists a row twice"));
            }
        }
        Ok(problems)
    });
    match outcome {
        Ok(problems) => problems.iter().for_each(|problem| findings.report("lists", problem)),
        Err(error) => findings.report("lists", format!("check failed: {error}")),
    }
}

fn stale_cache_entries(db: &Db, universe: &[(String, String)]) -> Result<Vec<String>, DbError> {
    let _ = db.expire_ephemeral();
    db.shared.read(|reader| {
        let mut stale = Vec::new();
        for model in MODELS {
            let ids: Vec<String> = universe.iter().filter(|(owner, _)| owner == model).map(|(_, id)| id.clone()).collect();
            if ids.is_empty() {
                continue;
            }
            let spec = db.shared.spec(model)?;
            let stored = reader.stored_rows(model, &ids)?;
            let mut syncs = read_sync(reader.connection(), model, &ids)?;
            let overlay = (!spec.ephemeral.is_empty()).then(|| db.shared.ephemeral.lock());
            for id in &ids {
                let cached = db.shared.cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).get(model, id);
                let fresh = stored.get(id).and_then(|record| {
                    let mut data = record.row.clone()?;
                    let ephemeral = overlay.as_ref().map_or(0, |state| state.overlay(spec, id, &mut data));
                    Some((record.revision, data, syncs.remove(id).unwrap_or_default(), ephemeral))
                });
                match (cached, fresh) {
                    (Some(row), Some((revision, data, sync, ephemeral))) => {
                        if row.revision != revision || row.data != data || row.sync != sync || row.ephemeral != ephemeral {
                            stale.push(format!("{model}/{id}: cached revision {} data {:?} sync {:?} vs stored revision {revision} data {data:?} sync {sync:?}", row.revision, row.data, row.sync.state));
                        }
                    }
                    (Some(row), None) => stale.push(format!("{model}/{id}: cached revision {} but the row is not stored", row.revision)),
                    (None, _) => {}
                }
            }
        }
        Ok(stale)
    })
}

pub fn cache(db: &Db, universe: &Universe, findings: &Findings) {
    let ids: Vec<(String, String)> = universe.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).iter().cloned().collect();
    let mut last = Vec::new();
    for attempt in 0..CACHE_ATTEMPTS {
        match stale_cache_entries(db, &ids) {
            Ok(stale) if stale.is_empty() => return,
            Ok(stale) => last = stale,
            Err(error) => {
                findings.report("cache", format!("check failed: {error}"));
                return;
            }
        }
        if attempt + 1 < CACHE_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(4));
        }
    }
    last.iter().take(5).for_each(|entry| findings.report("cache", entry));
}
