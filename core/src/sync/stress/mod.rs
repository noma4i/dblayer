mod check;
mod dice;
mod findings;
mod observers;
mod oracle;
mod server;
mod watchdog;
mod workload;
mod world;

#[cfg(test)]
mod check_tests;
#[cfg(test)]
mod observers_tests;
#[cfg(test)]
mod oracle_tests;
#[cfg(test)]
mod run_tests;
#[cfg(test)]
mod server_tests;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Map;
use tokio::runtime::{Builder, Runtime};

use crate::db::Db;
use crate::identity::ScopeKey;
use crate::pacing::Backoff;
use crate::transport::{EventSpec, MockTransport};

use super::rig::{rig_on, specs};
use super::{EventEffect, LoadOptions, OperationDef, SubscriptionDef, SyncConfig};

use findings::Findings;
use observers::{Aliases, Universe, Watchers};
use server::FakeServer;
use watchdog::Progress;
use workload::{judge, name_of, Worker};
use world::World;

const STEP_LIMIT: Duration = Duration::from_secs(30);
const CHECK_EVERY: u64 = 25;
const DRAIN_ROUNDS: usize = 40;
const OWNED_ROWS: usize = 2;
const SHARED_ROWS: usize = 3;
const MINIMIZE_BUDGET: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct StressConfig {
    pub seed: u64,
    pub threads: usize,
    pub seconds: u64,
    pub ops: Option<u64>,
    pub trace: bool,
}

impl StressConfig {
    pub fn command(&self, ops: Option<u64>) -> String {
        let limit = ops.map_or(format!("--seconds {}", self.seconds), |ops| format!("--ops {ops}"));
        format!("cargo run -p dblayer --features stress --bin stress -- --seed {} --threads {} {limit}", self.seed, self.threads)
    }

    fn workers(&self) -> usize {
        self.threads.max(1)
    }
}

#[derive(Debug, Clone, Default)]
pub struct StressReport {
    pub findings: Vec<String>,
    pub ops: Vec<u64>,
    pub digest: String,
    pub requests: u64,
}

pub fn scope_of(db: &Db, relation: &str) -> ScopeKey {
    let spec = db.spec("moments").expect("moments model");
    let relation = spec.relations.iter().find(|candidate| candidate.name == relation).expect("relation");
    relation.scope_key("moments", &BTreeMap::new()).expect("global relation key")
}

fn scratch_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("stress upload directory")
}

fn runtime(threads: usize) -> Runtime {
    if threads <= 1 {
        Builder::new_current_thread().enable_time().start_paused(true).build().expect("runtime")
    } else {
        Builder::new_multi_thread().worker_threads(threads).enable_time().build().expect("runtime")
    }
}

pub fn run(config: &StressConfig) -> StressReport {
    let progress = Progress::new(config.workers());
    watchdog::watch(progress.clone(), config.command(config.ops));
    let report = runtime(config.threads).block_on(scenario(config, progress.clone()));
    progress.finish();
    report
}

pub fn run_checked(config: &StressConfig) -> StressReport {
    let mut report = run(config);
    if config.threads <= 1 && report.findings.is_empty() {
        let again = run(&StressConfig { ops: Some(report.ops.iter().copied().max().unwrap_or(0)), seconds: 3600, ..config.clone() });
        if again.digest != report.digest {
            report.findings.push(format!("[determinism] two single-thread runs of seed {} ended differently:\n{}\n---\n{}", config.seed, report.digest, again.digest));
        }
    }
    report
}

fn tag_of(finding: &str) -> &str {
    finding.split(']').next().unwrap_or(finding)
}

pub fn shrink(reached: u64, mut fails: impl FnMut(u64) -> bool, budget: Duration) -> Option<u64> {
    let started = Instant::now();
    let (mut low, mut high) = (1, reached.max(1));
    if !fails(high) {
        return None;
    }
    while low < high && started.elapsed() < budget {
        let middle = low + (high - low) / 2;
        if fails(middle) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    Some(high)
}

pub fn minimize(config: &StressConfig, failing: &StressReport) -> Option<u64> {
    let wanted = tag_of(failing.findings.first()?).to_string();
    let attempts = if config.threads <= 1 { 1 } else { 4 };
    let fails = |ops: u64| {
        (0..attempts).any(|_| {
            let trial = run(&StressConfig { ops: Some(ops), seconds: 3600, ..config.clone() });
            trial.findings.iter().any(|finding| tag_of(finding) == wanted)
        })
    };
    shrink(failing.ops.iter().copied().max().unwrap_or(0), fails, MINIMIZE_BUDGET)
}

fn rows_line(world: &World) -> String {
    let rows = world.rig.db.query(&crate::query::Query::new("moments")).unwrap_or_default();
    rows.iter().map(|row| format!("{}={}", row.id.to_string().chars().take(9).collect::<String>(), row.text("text").unwrap_or_default())).collect::<Vec<_>>().join(" ")
}

fn outbox_line(world: &World) -> String {
    let entries = world.rig.db.outbox().unwrap_or_default();
    entries
        .iter()
        .map(|entry| {
            let operation = &entry.operation;
            let ids: Vec<String> = operation.row_ids.iter().map(|id| id.to_string().chars().take(9).collect()).collect();
            format!("{}#{} {:?}{} {}", operation.action_key.replace("moments:", ""), operation.id.0, operation.status, if operation.transport_started { "*" } else { "" }, ids.join(","))
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn dump(world: &World, progress: &Progress) -> String {
    let db = &world.rig.db;
    let queue: Vec<String> = db
        .outbox()
        .unwrap_or_default()
        .iter()
        .map(|entry| {
            let operation = &entry.operation;
            let upload = operation.upload.as_ref().map_or(String::new(), |state| format!(" upload {:?} {:?} retries {} sent {}/{}", state.phase, state.stage, state.attempts(), state.sent_bytes, state.total_bytes));
            format!("  seq {} {} {:?} rows {:?} temp {:?} attempts {} started {}{upload}", operation.id.0, operation.action_key, operation.status, operation.row_ids, operation.temp_id, operation.attempts, operation.transport_started)
        })
        .collect();
    format!("{}\noutbox ({} open):\n{}\ncounters {:?}\nobservers {:?}\nserver requests {}", progress.summary(), queue.len(), queue.join("\n"), db.counters(), db.observer_census(), world.server.received())
}

fn subscription() -> SubscriptionDef {
    let event = EventSpec::new("moment_changed").debounced(Duration::from_millis(3)).merged_by_row();
    SubscriptionDef::new(event, "MomentChanged", "moments", EventEffect::Upsert)
}

async fn worker_loop(id: usize, config: StressConfig, world: Arc<World>, progress: Arc<Progress>, stop: Arc<AtomicBool>) {
    let deadline = Instant::now() + Duration::from_secs(config.seconds);
    let mut worker = Worker::new(id, config.seed);
    let mut done = 0u64;
    while !stop.load(Ordering::SeqCst) && config.ops.is_none_or(|limit| done < limit) && Instant::now() < deadline {
        let op = worker.pick();
        let label = name_of(op);
        progress.begin(id, label);
        world.findings.trace(|| format!("worker {id} op {done} {label}"));
        if tokio::time::timeout(STEP_LIMIT, worker.run(&world, op)).await.is_err() {
            world.findings.report("hang", format!("worker {id} did not finish {label} in {} s\n{}", STEP_LIMIT.as_secs(), dump(&world, &progress)));
            stop.store(true, Ordering::SeqCst);
            return;
        }
        progress.end(id);
        done += 1;
        world.findings.trace(|| format!("outbox {}", outbox_line(&world)));
        world.findings.trace(|| format!("rows {}", rows_line(&world)));
        if done.is_multiple_of(CHECK_EVERY) {
            check::queue(&world.rig.db, &world.findings);
            check::lists(&world.rig.db, &world.findings);
            check::cache(&world.rig.db, &world.universe, &world.findings);
        }
        tokio::task::yield_now().await;
    }
}

async fn drain(world: &World, progress: &Progress) {
    world.go_offline(false);
    world.server.set_calm(true);
    let mut waited = 0;
    while world.pending_events.load(Ordering::SeqCst) > 0 && waited < 400 {
        tokio::time::sleep(Duration::from_millis(5)).await;
        waited += 1;
    }
    for round in 0..DRAIN_ROUNDS {
        world.rig.clock.advance(Duration::from_secs(3600));
        let _call = world.server.note_retry();
        let step = async {
            judge("drain retry_all", world.rig.sync.retry_all().await.map(|_| ()), world);
            world.rig.sync.wait_uploads().await;
        };
        if tokio::time::timeout(STEP_LIMIT, step).await.is_err() {
            world.findings.report("hang", format!("drain round {round} did not finish in {} s\n{}", STEP_LIMIT.as_secs(), dump(world, progress)));
            return;
        }
        world.rig.db.settle();
        tokio::time::sleep(Duration::from_millis(5)).await;
        if world.rig.db.outbox().map(|queue| queue.is_empty()).unwrap_or(false) {
            return;
        }
    }
    world.findings.report("stuck-queue", format!("operations are still open after {DRAIN_ROUNDS} calm rounds\n{}", dump(world, progress)));
}

async fn reload(world: &World) {
    let params = Map::new();
    for relation in ["empty_aware", "feed", "history"] {
        let _ = world.rig.sync.load("moments", relation, &params, LoadOptions { force: true }).await;
    }
    for _ in 0..8 {
        let _ = world.rig.sync.load_more("moments", "feed", &params).await;
    }
    world.rig.db.settle();
}

async fn scenario(config: &StressConfig, progress: Arc<Progress>) -> StressReport {
    let workers = config.workers();
    let findings = Findings::default().tracing(config.trace);
    let transport = MockTransport::new();
    let server = FakeServer::install(&transport, config.seed, findings.clone());
    server.seed_rows(workers, OWNED_ROWS, SHARED_ROWS);
    let mut sync_config = SyncConfig::default();
    sync_config.upload.backoff = Backoff::new(Duration::from_millis(5), Duration::from_millis(20)).limited(8);
    let rig = rig_on(Db::open_in_memory(specs(), None).expect("open db"), transport, sync_config);
    rig.sync.register(OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    rig.sync.register(OperationDef::new("CreateBlob", "operation CreateBlob { x }", "createBlob"));
    let universe = Universe::default();
    let aliases = Aliases::default();
    let rows = vec!["s1".to_string(), format!("s{}", workers * OWNED_ROWS + 1)];
    let watchers = Watchers::attach(&rig.db, &findings, &universe, &aliases, &rows, scope_of(&rig.db, "feed"));
    let scratch = scratch_dir();
    let world = Arc::new(World::new(rig, server, findings.clone(), universe, aliases, scratch.path().to_path_buf()));
    world.server.set_calm(true);
    let _ = world.rig.sync.load("moments", "empty_aware", &Map::new(), LoadOptions { force: true }).await;
    world.server.set_calm(false);
    let _events = world.rig.sync.subscribe(subscription()).await.expect("subscribe");
    let stop = Arc::new(AtomicBool::new(false));
    let handles: Vec<_> = (0..workers).map(|id| tokio::spawn(worker_loop(id, config.clone(), world.clone(), progress.clone(), stop.clone()))).collect();
    for handle in handles {
        if let Err(error) = handle.await {
            findings.report("panic", format!("a worker panicked: {error}"));
        }
    }
    let ops = progress.ops();
    drain(&world, &progress).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    world.rig.db.settle();
    let _ = world.rig.db.expire_ephemeral();
    check::queue(&world.rig.db, &findings);
    check::lists(&world.rig.db, &findings);
    check::cache(&world.rig.db, &world.universe, &findings);
    oracle::lost_writes(&world);
    oracle::settled_client(&world);
    reload(&world).await;
    oracle::reloaded_client(&world);
    check::cache(&world.rig.db, &world.universe, &findings);
    watchers.finish(&world.rig.db, &findings);
    let report = StressReport { findings: findings.all(), ops, digest: oracle::digest(&world), requests: world.server.received() };
    report
}
