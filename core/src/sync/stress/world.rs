use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::db::DbError;
use crate::sync::rig::Rig;

use super::findings::Findings;
use super::observers::{Aliases, Universe};
use super::server::FakeServer;

pub struct Issued {
    pub token: String,
    pub kind: &'static str,
    pub target: Option<String>,
}

pub struct World {
    pub rig: Rig,
    pub server: FakeServer,
    pub findings: Findings,
    pub universe: Universe,
    pub aliases: Aliases,
    pub offline: AtomicBool,
    pub moments_reset: AtomicBool,
    pub pending_events: Arc<AtomicUsize>,
    pub scratch: PathBuf,
    pub scratch_files: AtomicU64,
    issued: Mutex<Vec<Issued>>,
    created: Mutex<HashMap<String, String>>,
    discarded: Mutex<BTreeSet<String>>,
    touched: Mutex<BTreeSet<String>>,
}

impl World {
    pub fn new(rig: Rig, server: FakeServer, findings: Findings, universe: Universe, aliases: Aliases, scratch: PathBuf) -> Self {
        Self {
            rig,
            server,
            findings,
            universe,
            aliases,
            offline: AtomicBool::new(false),
            moments_reset: AtomicBool::new(false),
            pending_events: Arc::new(AtomicUsize::new(0)),
            scratch,
            scratch_files: AtomicU64::new(0),
            issued: Mutex::new(Vec::new()),
            created: Mutex::new(HashMap::new()),
            discarded: Mutex::new(BTreeSet::new()),
            touched: Mutex::new(BTreeSet::new()),
        }
    }

    fn locked<'a, T>(mutex: &'a Mutex<T>) -> MutexGuard<'a, T> {
        mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn issue(&self, token: &str, kind: &'static str, target: Option<&str>) {
        Self::locked(&self.issued).push(Issued { token: token.to_string(), kind, target: target.map(str::to_string) });
    }

    pub fn issued(&self) -> Vec<Issued> {
        Self::locked(&self.issued).iter().map(|issued| Issued { token: issued.token.clone(), kind: issued.kind, target: issued.target.clone() }).collect()
    }

    pub fn created_as(&self, id: &str, token: &str) {
        Self::locked(&self.created).insert(id.to_string(), token.to_string());
    }

    pub fn discarded(&self, id: &str) {
        Self::locked(&self.discarded).insert(id.to_string());
    }

    fn canonical(&self, id: &str) -> String {
        let aliases = Self::locked(&self.aliases);
        let mut current = id.to_string();
        while let Some(next) = aliases.get(&current) {
            current = next.clone();
        }
        current
    }

    pub fn was_discarded(&self, id: &str) -> bool {
        let wanted = self.canonical(id);
        Self::locked(&self.discarded).iter().any(|discarded| self.canonical(discarded) == wanted)
    }

    pub fn creator_of(&self, id: &str) -> Option<String> {
        Self::locked(&self.created).get(id).cloned()
    }

    pub fn touch(&self, id: &str) {
        Self::locked(&self.touched).insert(id.to_string());
    }

    pub fn touched(&self, id: &str) -> bool {
        Self::locked(&self.touched).contains(id)
    }

    pub fn is_offline(&self) -> bool {
        self.offline.load(Ordering::SeqCst)
    }

    pub fn go_offline(&self, offline: bool) {
        self.offline.store(offline, Ordering::SeqCst);
        self.server.set_offline(offline);
        self.rig.sync.set_online(!offline);
    }

    pub fn remember(&self, model: &str, id: &str) {
        Self::locked(&self.universe).insert((model.to_string(), id.to_string()));
    }

    pub fn replace(&self, request: &crate::db::ReplaceRequest, rows: Vec<serde_json::Map<String, serde_json::Value>>) {
        let outcome = self.rig.db.replace(request, rows);
        if request.on_absent == crate::db::OnAbsent::Destroy {
            if let Ok(result) = &outcome {
                for change in &result.rows {
                    if change.kind == crate::commit::RowChangeKind::Destroyed {
                        self.touch(change.row.id.as_str());
                    }
                }
            }
        }
        self.expect("replace", outcome.map(|_| ()));
    }

    pub fn expect(&self, step: &str, outcome: Result<(), DbError>) {
        if let Err(error) = outcome {
            let racing_a_reset = error.to_string().contains("generation is stale");
            if !matches!(error, DbError::NotFound { .. } | DbError::InFlight { .. }) && !racing_a_reset {
                self.findings.report("api-error", format!("{step}: {error}"));
            }
        }
    }
}
