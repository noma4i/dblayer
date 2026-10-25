use std::sync::atomic::Ordering;
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::commit::ScopeRef;
use crate::db::{OnAbsent, ReplaceRequest, Row};
use crate::identity::{ModelKey, RowId};
use crate::query::Query;
use crate::schema::Coverage;
use crate::sync::rig::params;
use crate::sync::{LoadOptions, SyncError};
use crate::transport::{Response, SubscriptionEvent};
use crate::upload::{UploadInput, UploadSteps};

use super::dice::Dice;
use super::world::World;

#[derive(Clone, Copy)]
pub enum Op {
    Create,
    Update,
    Like,
    Destroy,
    Flush,
    Load,
    LoadMore,
    History,
    Complete,
    Refresh,
    Replace,
    Retry,
    RetryAll,
    Discard,
    Typing,
    Upload,
    Reset,
    Connectivity,
    Tick,
    Event,
    Yield,
}

const TABLE: [(u64, Op); 21] = [
    (14, Op::Create),
    (16, Op::Update),
    (4, Op::Like),
    (6, Op::Destroy),
    (18, Op::Flush),
    (5, Op::Load),
    (4, Op::LoadMore),
    (3, Op::History),
    (3, Op::Complete),
    (3, Op::Refresh),
    (4, Op::Replace),
    (4, Op::Retry),
    (2, Op::RetryAll),
    (3, Op::Discard),
    (3, Op::Typing),
    (2, Op::Upload),
    (1, Op::Reset),
    (2, Op::Connectivity),
    (3, Op::Tick),
    (5, Op::Event),
    (2, Op::Yield),
];

pub fn name_of(op: Op) -> &'static str {
    match op {
        Op::Create => "create",
        Op::Update => "update",
        Op::Like => "like",
        Op::Destroy => "destroy",
        Op::Flush => "flush",
        Op::Load => "load",
        Op::LoadMore => "load_more",
        Op::History => "history",
        Op::Complete => "complete_load",
        Op::Refresh => "refresh",
        Op::Replace => "replace",
        Op::Retry => "retry",
        Op::RetryAll => "retry_all",
        Op::Discard => "discard",
        Op::Typing => "typing",
        Op::Upload => "upload",
        Op::Reset => "reset",
        Op::Connectivity => "connectivity",
        Op::Tick => "tick",
        Op::Event => "event",
        Op::Yield => "yield",
    }
}

pub struct Worker {
    pub id: usize,
    dice: Dice,
    issue: u64,
}

pub fn judge(step: &str, outcome: Result<(), SyncError>, world: &World) {
    if let Err(error @ (SyncError::Store(_) | SyncError::Malformed(_) | SyncError::Params(_))) = outcome {
        if !matches!(&error, SyncError::Params(message) if message.contains("already has an upload")) && !error.to_string().contains("generation is stale") {
            world.findings.report("api-error", format!("{step}: {error}"));
        }
    }
}

fn no_params() -> Map<String, Value> {
    Map::new()
}

impl Worker {
    pub fn new(id: usize, seed: u64) -> Self {
        Self { id, dice: Dice::for_worker(seed, id), issue: 0 }
    }

    fn next_text(&mut self) -> String {
        self.issue += 1;
        format!("w{}:{}", self.id, self.issue)
    }

    pub fn pick(&mut self) -> Op {
        let total: u64 = TABLE.iter().map(|(weight, _)| weight).sum();
        let mut roll = self.dice.below(total);
        for (weight, op) in TABLE {
            if roll < weight {
                return op;
            }
            roll -= weight;
        }
        Op::Yield
    }

    fn own_rows(&self, world: &World) -> Vec<Row> {
        let prefix = format!("w{}:", self.id);
        let rows = match world.rig.db.query(&Query::new("moments")) {
            Ok(rows) => rows,
            Err(error) => {
                world.findings.report("workload-read", error.to_string());
                return Vec::new();
            }
        };
        let mut rows: Vec<Row> = rows.into_iter().filter(|row| row.text("text").is_some_and(|text| text.starts_with(&prefix))).collect();
        rows.sort_by_key(|row| (row.text("text").map(str::to_string), row.is_local()));
        rows
    }

    fn pick_own(&mut self, world: &World) -> Option<Row> {
        let rows = self.own_rows(world);
        self.dice.pick(&rows).cloned()
    }

    pub async fn run(&mut self, world: &World, op: Op) {
        let rig = &world.rig;
        match op {
            Op::Create => {
                let text = self.next_text();
                let outcome = rig.db.create("moments", params(json!({"text": text})));
                world.findings.trace(|| format!("issued create {text} -> {:?}", outcome.as_ref().map(|row| row.id.to_string())));
                if let Ok(row) = &outcome {
                    world.issue(&text, "create", Some(row.id.as_str()));
                    world.remember("moments", row.id.as_str());
                    world.created_as(row.id.as_str(), &text);
                }
                world.expect("create", outcome.map(|_| ()));
            }
            Op::Update => {
                let Some(row) = self.pick_own(world) else { return };
                let text = self.next_text();
                let outcome = rig.db.update("moments", &row.id, params(json!({"text": text})));
                if outcome.is_ok() {
                    world.issue(&text, "update", Some(row.id.as_str()));
                    world.findings.trace(|| format!("issued update {text} on {}", row.id));
                }
                world.expect("update", outcome.map(|_| ()));
            }
            Op::Like => {
                let Some(row) = self.pick_own(world) else { return };
                let outcome = rig.db.act("moments", "like", &row.id, params(json!({"likedByMe": true})), no_params());
                world.expect("like", outcome.map(|_| ()));
            }
            Op::Destroy => {
                let Some(row) = self.pick_own(world) else { return };
                world.findings.trace(|| format!("issued destroy of {} {:?}", row.id, row.text("text")));
                let outcome = rig.db.destroy("moments", &row.id);
                if outcome.is_ok() && row.is_local() && !rig.db.outbox().unwrap().iter().any(|entry| entry.operation.row_ids.contains(&row.id)) {
                    world.discarded(row.id.as_str());
                }
                world.expect("destroy", outcome.map(|_| ()));
            }
            Op::Flush => {
                let report = rig.sync.flush().await;
                world.findings.trace(|| format!("flush report {:?}", report.as_ref().map(|report| report.operations.iter().map(|sent| format!("{}:{:?}", sent.sequence, sent.result)).collect::<Vec<_>>())));
                self.report("flush", report.map(|_| ()), world);
            }
            Op::Load => {
                let options = LoadOptions { force: self.dice.percent(60) };
                self.load(world, "feed", options).await;
            }
            Op::LoadMore => {
                if !world.is_offline() {
                    self.report("load_more", rig.sync.load_more("moments", "feed", &no_params()).await.map(|_| ()), world);
                }
            }
            Op::History => self.load(world, "history", LoadOptions { force: true }).await,
            Op::Complete => self.load(world, "empty_aware", LoadOptions { force: true }).await,
            Op::Refresh => {
                if !world.is_offline() {
                    self.report("refresh", rig.sync.refresh("moments", "feed", &no_params()).await.map(|_| ()), world);
                }
            }
            Op::Replace => self.replace(world),
            Op::Retry => {
                let Some(row) = self.pick_own(world) else { return };
                let _call = world.server.note_retry();
                self.report("retry", rig.sync.retry("moments", &row.id).await.map(|_| ()), world);
            }
            Op::RetryAll => {
                let _call = world.server.note_retry();
                self.report("retry_all", rig.sync.retry_all().await.map(|_| ()), world);
            }
            Op::Discard => {
                let Some(row) = self.pick_own(world) else { return };
                world.findings.trace(|| format!("discard of {} {:?} {:?}", row.id, row.text("text"), row.sync.state));
                world.discarded(row.id.as_str());
                world.expect("discard", rig.db.discard("moments", &row.id).map(|_| ()));
            }
            Op::Typing => {
                let user = RowId::new(&format!("u{}", self.dice.below(3))).expect("user id");
                world.remember("users", user.as_str());
                let flag = self.dice.percent(50);
                world.expect("typing", rig.db.set_ephemeral("users", &user, params(json!({"typing": flag}))));
            }
            Op::Upload => self.upload(world).await,
            Op::Reset => {
                let models: &[&str] = match self.dice.below(3) {
                    0 => &["vibes"],
                    1 => &["users"],
                    _ => &["moments"],
                };
                world.findings.trace(|| format!("reset {models:?}"));
                world.reset(models);
            }
            Op::Connectivity => {
                let offline = !world.is_offline();
                world.go_offline(offline && self.dice.percent(70));
            }
            Op::Tick => {
                rig.clock.advance(Duration::from_secs(1 + self.dice.below(90)));
                world.expect("expire", rig.db.expire_ephemeral().map(|_| ()));
                if self.dice.percent(40) {
                    world.go_offline(false);
                }
            }
            Op::Event => self.event(world),
            Op::Yield => tokio::task::yield_now().await,
        }
    }

    fn report(&self, step: &str, outcome: Result<(), SyncError>, world: &World) {
        judge(step, outcome, world);
    }

    async fn load(&mut self, world: &World, relation: &str, options: LoadOptions) {
        if world.is_offline() {
            return;
        }
        self.report(relation, world.rig.sync.load("moments", relation, &no_params(), options).await.map(|_| ()), world);
    }

    fn noise_target(&mut self, world: &World) -> Option<String> {
        let ids = world.server.ids();
        let shared: Vec<&String> = ids.iter().filter(|id| world.server.node_of(id).is_some_and(|node| node["text"].as_str().is_some_and(|text| text.starts_with("n:")))).collect();
        let id = if self.dice.percent(75) { self.dice.pick(&shared).map(|id| (*id).clone()) } else { self.dice.pick(&ids).cloned() }?;
        if !shared.contains(&&id) {
            world.touch(&id);
        }
        Some(id)
    }

    fn replace(&mut self, world: &World) {
        let mut rows = Vec::new();
        for _ in 0..1 + self.dice.below(4) {
            let Some(id) = self.noise_target(world) else { break };
            if let Some(Value::Object(node)) = world.server.node_of(&id) {
                rows.push(node);
            }
        }
        world.findings.trace(|| format!("replace rows {rows:?}"));
        let scope = ScopeRef::new(ModelKey::new("moments"), super::scope_of(&world.rig.db, "feed"));
        let mut request = ReplaceRequest::new(scope, if self.dice.percent(50) { Coverage::Page } else { Coverage::Complete });
        request.on_absent = if self.dice.percent(50) { OnAbsent::Destroy } else { OnAbsent::Detach };
        world.replace(&request, rows);
    }

    fn event(&mut self, world: &World) {
        let Some(id) = self.noise_target(world) else { return };
        let Some(node) = world.server.node_of(&id) else { return };
        let copies = 1 + usize::from(self.dice.percent(30));
        world.findings.trace(|| format!("event planned {node} copies {copies}"));
        let delay = if self.dice.percent(50) { Duration::from_millis(1 + self.dice.below(25)) } else { Duration::ZERO };
        let transport = world.rig.transport.clone();
        let findings = world.findings.clone();
        let emit = move || {
            findings.trace(|| format!("event delivered {node}"));
            for _ in 0..copies {
                transport.emit("MomentChanged", SubscriptionEvent::Next(Response::data(json!({"momentChanged": node.clone()}))));
            }
        };
        if delay.is_zero() {
            emit();
            return;
        }
        let pending = world.pending_events.clone();
        pending.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            emit();
            pending.fetch_sub(1, Ordering::SeqCst);
        });
    }

    async fn upload(&mut self, world: &World) {
        let Some(row) = self.pick_own(world) else { return };
        let text = self.next_text();
        let path = world.scratch.join(format!("upload-{}.bin", world.scratch_files.fetch_add(1, Ordering::SeqCst)));
        if std::fs::write(&path, b"abcdef").is_err() {
            return;
        }
        let steps = UploadSteps::new("CreateMoment").single("CreateBlob");
        let input = UploadInput::new(&path.to_string_lossy(), steps).variable("text", json!(text));
        let outcome = world.rig.sync.upload("moments", &row.id, "upload", input).await;
        if outcome.is_ok() {
            world.issue(&text, "upload", Some(row.id.as_str()));
            world.findings.trace(|| format!("issued upload {text} on {}", row.id));
        }
        self.report("upload", outcome.map(|_| ()), world);
    }
}
