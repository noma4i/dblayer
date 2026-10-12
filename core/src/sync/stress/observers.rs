use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::db::{Change, CountChange, Db, ObserverHandle};
use crate::identity::{RowId, ScopeKey};
use crate::query::{Query, Where};

use super::findings::Findings;

pub type Universe = Arc<Mutex<BTreeSet<(String, String)>>>;
pub type Aliases = Arc<Mutex<HashMap<String, String>>>;

#[derive(Clone)]
enum Target {
    Rows(Query),
    Row(String, RowId),
    Count(Query),
}

struct RowsTape {
    last: Option<Change>,
    seen: u64,
}

struct CountTape {
    last: Option<CountChange>,
    seen: u64,
}

pub struct Watcher {
    name: String,
    target: Target,
    rows: Mutex<RowsTape>,
    count: Mutex<CountTape>,
}

pub struct Watchers {
    watchers: Vec<Arc<Watcher>>,
    handles: Vec<ObserverHandle>,
}

fn entries(change: &Change) -> Vec<(String, i64)> {
    change.rows.iter().map(|row| (row.id.to_string(), row.revision)).collect()
}

pub(super) fn check_step(name: &str, previous: Option<&Change>, next: &Change, findings: &Findings) {
    let ids: HashSet<&RowId> = next.rows.iter().map(|row| &row.id).collect();
    if ids.len() != next.rows.len() {
        findings.report("observer-duplicates", format!("{name}: revision {} lists a row twice", next.revision));
    }
    let Some(previous) = previous else {
        if next.diff.inserted.len() != next.rows.len() {
            findings.report("observer-diff", format!("{name}: first change inserts {} of {} rows", next.diff.inserted.len(), next.rows.len()));
        }
        return;
    };
    if next.revision < previous.revision {
        findings.report("observer-monotonic", format!("{name}: revision {} arrived after {}", next.revision, previous.revision));
    }
    let gone = |id: &str| next.diff.removed.iter().any(|row| row.as_str() == id) || next.diff.replaced.iter().any(|(from, _)| from.as_str() == id);
    let mut expected: Vec<String> = entries(previous).into_iter().map(|(id, _)| id).filter(|id| !gone(id)).collect();
    expected.extend(next.diff.inserted.iter().map(RowId::to_string));
    expected.extend(next.diff.replaced.iter().map(|(_, to)| to.to_string()));
    let mut actual: Vec<String> = entries(next).into_iter().map(|(id, _)| id).collect();
    expected.sort();
    actual.sort();
    if expected != actual {
        findings.report("observer-diff", format!("{name}: diff from revision {} to {} does not lead to the delivered rows ({expected:?} vs {actual:?})", previous.revision, next.revision));
    }
    let before: HashMap<String, i64> = entries(previous).into_iter().collect();
    for (id, revision) in entries(next) {
        let marked = next.diff.updated.iter().any(|row| row.as_str() == id) || next.diff.inserted.iter().any(|row| row.as_str() == id) || next.diff.replaced.iter().any(|(_, to)| to.as_str() == id);
        if before.get(&id).is_some_and(|old| *old != revision) && !marked {
            findings.report("observer-diff", format!("{name}: row {id} changed to revision {revision} without being marked updated"));
        }
    }
}

impl Watcher {
    fn rows_listener(self: &Arc<Self>, findings: Findings, universe: Universe, aliases: Aliases) -> impl Fn(&Change) + Send + Sync + 'static {
        let watcher = self.clone();
        move |change: &Change| {
            let mut tape = watcher.rows.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut rows = tape.last.as_ref().map(|last| last.rows.clone()).unwrap_or_default();
            change.apply_to(&mut rows);
            let change = &Change { rows, full: true, ..change.clone() };
            check_step(&watcher.name, tape.last.as_ref(), change, &findings);
            let mut seen = universe.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            for row in &change.rows {
                seen.insert((watcher.model(), row.id.to_string()));
            }
            let mut moved = aliases.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            for (from, to) in &change.diff.replaced {
                moved.insert(from.to_string(), to.to_string());
            }
            tape.seen += 1;
            tape.last = Some(change.clone());
        }
    }

    fn count_listener(self: &Arc<Self>, findings: Findings) -> impl Fn(&CountChange) + Send + Sync + 'static {
        let watcher = self.clone();
        move |change: &CountChange| {
            let mut tape = watcher.count.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(previous) = tape.last {
                if change.revision < previous.revision {
                    findings.report("observer-monotonic", format!("{}: count revision {} arrived after {}", watcher.name, change.revision, previous.revision));
                }
                if change.count == previous.count {
                    findings.report("observer-diff", format!("{}: count {} delivered twice", watcher.name, change.count));
                }
            }
            tape.seen += 1;
            tape.last = Some(*change);
        }
    }

    fn model(&self) -> String {
        match &self.target {
            Target::Rows(query) | Target::Count(query) => query.model.as_str().to_string(),
            Target::Row(model, _) => model.clone(),
        }
    }

    fn fresh(&self, db: &Db) -> Vec<(String, i64)> {
        match &self.target {
            Target::Rows(query) => db.query(query).unwrap_or_default().iter().map(|row| (row.id.to_string(), row.revision)).collect(),
            Target::Row(model, id) => db.find(model, id).ok().flatten().iter().map(|row| (row.id.to_string(), row.revision)).collect(),
            Target::Count(_) => Vec::new(),
        }
    }

    fn finish(&self, db: &Db, findings: &Findings) -> u64 {
        if let Target::Count(query) = &self.target {
            let tape = self.count.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let fresh = db.count(query).unwrap_or(-1);
            if tape.last.map(|change| change.count) != Some(fresh) && !(tape.last.is_none() && fresh == 0) {
                findings.report("observer-final", format!("{}: last count {:?} differs from the fresh count {fresh}", self.name, tape.last.map(|change| change.count)));
            }
            return tape.seen;
        }
        let tape = self.rows.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let fresh = self.fresh(db);
        let last = tape.last.as_ref().map(entries).unwrap_or_default();
        if last != fresh {
            findings.report("observer-final", format!("{}: last delivered state {last:?} differs from the fresh read {fresh:?}", self.name));
            return tape.seen;
        }
        let fresh_rows = match &self.target {
            Target::Rows(query) => db.query(query).unwrap_or_default(),
            Target::Row(model, id) => db.find(model, id).ok().flatten().into_iter().collect(),
            Target::Count(_) => Vec::new(),
        };
        let delivered = tape.last.as_ref().map(|change| change.rows.clone()).unwrap_or_default();
        if fresh_rows.iter().zip(&delivered).any(|(fresh, seen)| **fresh != **seen) {
            findings.report("observer-final", format!("{}: delivered rows carry different data, sync state or ephemeral values than the fresh read", self.name));
        }
        tape.seen
    }
}

impl Watchers {
    pub fn attach(db: &Db, findings: &Findings, universe: &Universe, aliases: &Aliases, row_ids: &[String], feed: ScopeKey) -> Self {
        let mut targets = vec![
            ("all moments", Target::Rows(Query::new("moments"))),
            ("liked moments", Target::Rows(Query::new("moments").filtered(Where::equal("likedByMe", serde_json::json!(true))))),
            ("feed scope", Target::Rows(Query::new("moments").in_scope(feed))),
            ("moments with author", Target::Rows(Query::new("moments").including(&["author"]))),
            ("moment count", Target::Count(Query::new("moments"))),
            ("users", Target::Rows(Query::new("users"))),
        ];
        let mut named: Vec<(String, Target)> = targets.drain(..).map(|(name, target)| (name.to_string(), target)).collect();
        for id in row_ids {
            named.push((format!("moment {id}"), Target::Row("moments".to_string(), RowId::new(id).expect("seed id"))));
        }
        named.push(("user u0".to_string(), Target::Row("users".to_string(), RowId::new("u0").expect("user id"))));
        let mut watchers = Vec::new();
        let mut handles = Vec::new();
        for (name, target) in named {
            let watcher = Arc::new(Watcher { name, target: target.clone(), rows: Mutex::new(RowsTape { last: None, seen: 0 }), count: Mutex::new(CountTape { last: None, seen: 0 }) });
            let handle = match &target {
                Target::Rows(query) => db.observe(query.clone(), watcher.rows_listener(findings.clone(), universe.clone(), aliases.clone())),
                Target::Row(model, id) => db.observe_row(model, id, watcher.rows_listener(findings.clone(), universe.clone(), aliases.clone())),
                Target::Count(query) => db.observe_count(query.clone(), watcher.count_listener(findings.clone())),
            };
            handles.push(handle.expect("attach observer"));
            watchers.push(watcher);
        }
        Self { watchers, handles }
    }

    pub fn finish(&self, db: &Db, findings: &Findings) -> u64 {
        let _ = &self.handles;
        self.watchers.iter().map(|watcher| watcher.finish(db, findings)).sum()
    }
}
