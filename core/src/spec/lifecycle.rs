use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::json;
use tokio::task::JoinHandle;

use super::harness_life::{kind, timeout, Rig, STEP_LIMIT};
use super::ref_lifecycle::LifecycleRef;
use crate::check::explore::{all_traces, seeded};
use crate::check::model::{run, Divergence, Owner, Property, RefModel, Sut};
use crate::check::value::{Input, Observation, PropertyRow, Step};
use crate::ffi::DbError;

pub fn input(name: &str) -> Input {
    Input::new(name)
}

pub fn data(epoch: &str) -> Input {
    Input::new("call_data").with("epoch", epoch)
}

pub fn create(name: &str) -> Input {
    Input::new("create").with("as", name)
}

fn outcome(result: Result<(), DbError>) -> serde_json::Value {
    match result {
        Ok(()) => json!({ "ok": true }),
        Err(error) => json!({ "error": kind(&error) }),
    }
}

/// The real core behind `Database`: the app side (server, credential store, transport) is mocked, every input is a public call.
#[derive(Default)]
pub struct LifecycleSut {
    rig: Option<Rig>,
    rows: BTreeMap<String, String>,
    step: u64,
    pending: Vec<(u64, JoinHandle<Result<(), DbError>>)>,
}

impl LifecycleSut {
    pub fn new() -> Self {
        Self::default()
    }

    async fn rig(&mut self) -> &Rig {
        if self.rig.is_none() {
            self.rig = Some(Rig::open().await);
        }
        self.rig.as_ref().expect("opened")
    }

    fn spawn(&mut self, id: u64, work: impl std::future::Future<Output = Result<(), DbError>> + Send + 'static) {
        self.pending.push((id, tokio::spawn(work)));
    }

    async fn settle(&mut self) -> Vec<Observation> {
        let mut done = Vec::new();
        for (id, mut handle) in std::mem::take(&mut self.pending) {
            match crate::test_wait::timeout(STEP_LIMIT, &mut handle).await {
                Ok(Ok(result)) => done.push((id, Some(result))),
                Ok(Err(_)) => done.push((id, Some(Err(timeout())))),
                Err(_) => {
                    handle.abort();
                    done.push((id, Some(Err(timeout()))));
                }
            }
        }
        done.sort_by_key(|(id, _)| *id);
        done.into_iter().map(|(id, result)| reply_of(id, result.unwrap_or(Ok(())))).collect()
    }

    fn state(&self) -> Vec<Observation> {
        let Some(rig) = &self.rig else { return Vec::new() };
        let out = vec![Observation::new("epoch", json!(rig.database.session_epoch())), Observation::new("identity", json!({ "set": rig.credentials.value().is_some() }))];
        out
    }
}

fn reply_of(id: u64, result: Result<(), DbError>) -> Observation {
    let mut value = outcome(result);
    value["id"] = json!(id);
    Observation::new("reply", value)
}

impl Drop for LifecycleSut {
    fn drop(&mut self) {
        for (_, handle) in &self.pending {
            handle.abort();
        }
        if let Some(rig) = self.rig.take() {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move { let _ = crate::test_wait::timeout(STEP_LIMIT, rig.database.close_database()).await; });
            }
        }
    }
}

#[async_trait]
impl Sut for LifecycleSut {
    async fn apply(&mut self, step: &Input) -> Vec<Observation> {
        self.step += 1;
        let id = self.step;
        if step.name == "idle" {
            let before = self.rig.as_ref().map_or(0, |rig| rig.server.seen().len());
            if let Some(rig) = &self.rig { crate::ffi::settled(&rig.database).await.expect("the database settles"); }
            let after = self.rig.as_ref().map_or(0, |rig| rig.server.seen().len());
            return if after == before { Vec::new() } else { vec![Observation::new("traffic", json!(after - before))] };
        }
        let mut observed: Vec<Observation> = Vec::new();
        let rig = self.rig().await;
        let database = rig.database.clone();
        match step.name.as_str() {
            "open" => {}
            "login" => {
                let (server, credentials) = (rig.server.clone(), rig.credentials.clone());
                let handle = Rig { server, credentials, database: database.clone() };
                observed.push(reply_of(id, handle.login().await.map(|_| ())));
            }
            "logout" => {
                let (server, credentials) = (rig.server.clone(), rig.credentials.clone());
                let handle = Rig { server, credentials, database: database.clone() };
                observed.push(reply_of(id, handle.logout().await.map(|_| ())));
            }
            "session_reset" => {
                let target = database.clone();
                self.spawn(id, async move { target.session_reset().await });
            }
            "close" => {
                let target = database.clone();
                self.spawn(id, async move { target.close_database().await });
            }
            "call_data" | "create" | "rows" => {
                let epoch = database.session_epoch() + if step.text("epoch") == Some("other") { 1000 } else { 0 };
                let rig = self.rig.as_ref().expect("opened");
                let key = if step.name == "call_data" { "call" } else if step.name == "create" { "created" } else { "rows" };
                let value = match step.name.as_str() {
                    "create" => {
                        let name = step.text("as").unwrap_or("a").to_string();
                        match crate::test_wait::timeout(STEP_LIMIT, rig.create(epoch, &name)).await {
                            Ok(Ok(row)) => {
                                self.rows.insert(name.clone(), row);
                                json!({ "row": name, "epoch": epoch })
                            }
                            Ok(Err(error)) => json!({ "error": kind(&error) }),
                            Err(_) => json!({ "error": "timeout" }),
                        }
                    }
                    "rows" => match crate::test_wait::timeout(STEP_LIMIT, rig.rows(epoch)).await {
                        Ok(Ok(ids)) => {
                            let mut present: Vec<&String> = self.rows.iter().filter(|(_, row)| ids.contains(row)).map(|(name, _)| name).collect();
                            present.sort();
                            json!({ "epoch": epoch, "present": present })
                        }
                        Ok(Err(error)) => json!({ "error": kind(&error) }),
                        Err(_) => json!({ "error": "timeout" }),
                    },
                    _ => match crate::test_wait::timeout(STEP_LIMIT, rig.rows(epoch)).await {
                        Ok(Ok(_)) => json!({ "ok": true }),
                        Ok(Err(error)) => json!({ "error": kind(&error) }),
                        Err(_) => json!({ "error": "timeout" }),
                    },
                };
                observed.push(Observation::new(key, value));
            }
            other => return vec![Observation::new("unsupported", json!(other))],
        }
        let mut all = self.settle().await;
        all.extend(observed);
        all.extend(self.state());
        all
    }
}

fn epochs(steps: &[Step]) -> Vec<Option<u64>> {
    steps.iter().map(|step| step.observed.iter().find(|o| o.kind == "epoch").and_then(|o| o.value.as_u64())).collect()
}

fn find_all<'a>(steps: &'a [Step], key: &'a str) -> impl Iterator<Item = (usize, &'a Observation)> {
    steps.iter().enumerate().flat_map(move |(index, step)| step.observed.iter().filter(move |o| o.kind == key).map(move |o| (index, o)))
}

/// P-LIFE-01: `Published.epoch` never decreases.
fn epoch_monotonic(steps: &[Step]) -> Result<(), String> {
    let mut last = 0;
    for (index, epoch) in epochs(steps).into_iter().enumerate() {
        let Some(epoch) = epoch else { continue };
        if epoch < last { return Err(format!("step {} epoch {epoch} after {last}", index + 1)); }
        last = epoch;
    }
    Ok(())
}

/// P-LIFE-02: after the epoch moved past e, no work stamped e appears.
fn no_work_after_end(steps: &[Step]) -> Result<(), String> {
    let mut highest = 0;
    for (index, step) in steps.iter().enumerate() {
        if let Some(epoch) = epochs(&steps[index..=index])[0] { highest = highest.max(epoch); }
        for observation in &step.observed {
            if !matches!(observation.kind.as_str(), "created" | "rows") { continue; }
            if let Some(stamp) = observation.value["epoch"].as_u64() {
                if stamp < highest { return Err(format!("step {} works in the ended epoch {stamp}, current {highest}", index + 1)); }
            }
        }
    }
    Ok(())
}

/// P-LIFE-03: a row read in an epoch was created in that epoch (the data of the earlier epoch is cleared before the later one works).
fn clear_before_start(steps: &[Step]) -> Result<(), String> {
    let mut born: BTreeMap<String, u64> = BTreeMap::new();
    for (index, step) in steps.iter().enumerate() {
        for observation in &step.observed {
            match observation.kind.as_str() {
                "created" if observation.value["row"].is_string() => {
                    born.insert(observation.value["row"].as_str().unwrap_or_default().to_string(), observation.value["epoch"].as_u64().unwrap_or(0));
                }
                "rows" => {
                    let epoch = observation.value["epoch"].as_u64().unwrap_or(0);
                    for name in observation.value["present"].as_array().into_iter().flatten().filter_map(|n| n.as_str()) {
                        if born.get(name) != Some(&epoch) { return Err(format!("step {} reads {name} of epoch {:?} in epoch {epoch}", index + 1, born.get(name))); }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// P-LIFE-04: a reply belongs to a command step, comes once, and a command issued while the store is free is answered in its own step.
fn reply_once(steps: &[Step]) -> Result<(), String> {
    let mut seen: Vec<u64> = Vec::new();
    let mut held = false;
    for (index, step) in steps.iter().enumerate() {
        match step.input.name.as_str() {
            "store_hold" => held = true,
            "store_release" => held = false,
            _ => {}
        }
        for (_, observation) in step.observed.iter().enumerate().filter(|(_, o)| o.kind == "reply") {
            let id = observation.value["id"].as_u64().unwrap_or(0);
            if seen.contains(&id) { return Err(format!("step {} answers {id} twice", index + 1)); }
            if id == 0 || id as usize > index + 1 || !matches!(steps[id as usize - 1].input.name.as_str(), "session_reset" | "close" | "login" | "logout" | "credential_other") { return Err(format!("step {} answers {id}, which is no command", index + 1)); }
            seen.push(id);
        }
        let issued = matches!(step.input.name.as_str(), "session_reset" | "close") && !held;
        if issued && !seen.contains(&(index as u64 + 1)) { return Err(format!("step {} left its command unanswered with the store free", index + 1)); }
    }
    Ok(())
}

/// P-LIFE-05: once `close` is answered Ok, every later call is refused `closed`.
fn close_drains(steps: &[Step]) -> Result<(), String> {
    let mut closed_at: Option<usize> = None;
    for (index, step) in steps.iter().enumerate() {
        if let Some(close) = closed_at {
            for observation in step.observed.iter().filter(|o| matches!(o.kind.as_str(), "call" | "created" | "rows")) {
                if observation.value["error"] != "closed" { return Err(format!("step {} serves a call after the close of step {}", index + 1, close + 1)); }
            }
        }
        for (reply_step, observation) in find_all(&steps[index..=index], "reply") {
            let id = observation.value["id"].as_u64().unwrap_or(0) as usize;
            let _ = reply_step;
            if id > 0 && steps[id - 1].input.name == "close" && observation.value["ok"] == true && closed_at.is_none() { closed_at = Some(index); }
        }
    }
    Ok(())
}

/// P-LIFE-06: the generation never decreases, and an answered clearing raised it above its value before the command.
fn generation(steps: &[Step]) -> Result<(), String> {
    let values: Vec<Option<u64>> = steps.iter().map(|step| step.observed.iter().find(|o| o.kind == "generation").and_then(|o| o.value.as_u64())).collect();
    let mut last = 0;
    for (index, value) in values.iter().enumerate() {
        let Some(value) = value else { continue };
        if *value < last { return Err(format!("step {} generation {value} after {last}", index + 1)); }
        last = *value;
    }
    for index in 0..steps.len() {
        for (_, observation) in find_all(&steps[index..=index], "reply") {
            let id = observation.value["id"].as_u64().unwrap_or(0) as usize;
            if id == 0 || observation.value["ok"] != true || !matches!(steps[id - 1].input.name.as_str(), "session_reset") { continue; }
            let before = if id >= 2 { values[id - 2].unwrap_or(0) } else { 0 };
            if let Some(now) = values[index] { if now <= before { return Err(format!("step {} answers a clearing, generation {now} not above {before}", index + 1)); } }
        }
    }
    Ok(())
}

/// P-LIFE-07: the credential goes only after a `session_reset` or a confirmed `logout` was issued.
fn session_kept(steps: &[Step]) -> Result<(), String> {
    let mut ended = false;
    let mut set = false;
    for (index, step) in steps.iter().enumerate() {
        if matches!(step.input.name.as_str(), "session_reset" | "logout") { ended = true; }
        let Some(now) = step.observed.iter().find(|o| o.kind == "identity").map(|o| o.value["set"] == true) else { continue };
        if set && !now && !ended { return Err(format!("step {} dropped the credential without a reset or logout", index + 1)); }
        if !now { ended = false; }
        set = now;
    }
    Ok(())
}

/// P-LIFE-08: an idle step shows nothing.
fn idle(steps: &[Step]) -> Result<(), String> {
    match steps.iter().enumerate().find(|(_, step)| step.input.name == "idle" && !step.observed.is_empty()) {
        Some((index, step)) => Err(format!("idle step {} showed {:?}", index + 1, step.observed)),
        None => Ok(()),
    }
}

pub const PROPERTIES: [PropertyRow; 8] = [
    ("P-LIFE-01", epoch_monotonic),
    ("P-LIFE-02", no_work_after_end),
    ("P-LIFE-03", clear_before_start),
    ("P-LIFE-04", reply_once),
    ("P-LIFE-05", close_drains),
    ("P-LIFE-06", generation),
    ("P-LIFE-07", session_kept),
    ("P-LIFE-08", idle),
];

pub struct LifecycleOwner {
    pub full: bool,
    pub only: Option<&'static str>,
    pub alphabet: Vec<Input>,
}

impl LifecycleOwner {
    pub fn public(only: Option<&'static str>) -> Self {
        let alphabet = vec![input("login"), input("session_reset"), input("logout"), input("close"), data("current"), data("other"), create("a"), input("rows"), input("idle")];
        Self { full: false, only, alphabet }
    }

    pub fn full(only: Option<&'static str>) -> Self {
        let mut alphabet = Self::public(None).alphabet;
        alphabet.extend([input("credential_other"), input("server_ended_stale"), input("store_hold"), input("store_ok"), input("store_fail"), input("store_release"), input("server_ended"), input("stray_cleared"), input("stray_models_cleared"), input("stray_clear_failed"), input("stray_drained"), Input::new("drain").with("owner", "Feed")]);
        Self { full: true, only, alphabet }
    }
}

#[async_trait]
impl Owner for LifecycleOwner {
    fn name(&self) -> &'static str {
        "lifecycle"
    }

    fn alphabet(&self) -> Vec<Input> {
        self.alphabet.clone()
    }

    fn model(&self) -> Box<dyn RefModel> {
        Box::new(LifecycleRef::new(self.full))
    }

    async fn real(&self) -> Box<dyn Sut> {
        Box::new(LifecycleSut::new())
    }

    fn properties(&self) -> Vec<Property> {
        PROPERTIES.iter().filter(|(name, _)| self.only.is_none_or(|only| only == *name)).map(|(name, check)| Property { name, check: *check }).collect()
    }
}

const TRACE_CAP: usize = 60;

/// The traces of a property test: every sequence to `depth`, thinned evenly to `TRACE_CAP`, then the seeded long traces.
pub fn traces(alphabet: &[Input], depth: usize, seeds: &[u64], length: usize) -> Vec<Vec<Input>> {
    let all = all_traces(alphabet, depth);
    let stride = all.len().div_ceil(TRACE_CAP).max(1);
    let mut picked: Vec<Vec<Input>> = all.into_iter().enumerate().filter(|(index, _)| index % stride == 0).map(|(_, trace)| trace).collect();
    picked.extend(seeds.iter().map(|seed| seeded(alphabet, *seed, length)));
    picked
}

pub async fn explore(owner: &LifecycleOwner, depth: usize, seeds: &[u64], length: usize, required: &[Vec<Input>]) -> Result<(), Box<Divergence>> {
    for inputs in required.iter().cloned().chain(traces(&owner.alphabet, depth, seeds, length)) {
        run(owner, owner.name(), &inputs).await?;
    }
    Ok(())
}


/// The same walk for any owner: the required traces, then the thinned traces to depth 4 and the seeded long ones.
pub async fn explore_inputs(owner: &super::store::StoreOwner, required: &[Vec<Input>]) -> Result<(), Box<Divergence>> {
    for inputs in required.iter().cloned().chain(traces(&owner.alphabet, 4, &[1, 2], 30)) {
        run(owner, owner.name(), &inputs).await?;
    }
    Ok(())
}
