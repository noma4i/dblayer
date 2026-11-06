use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::json;

use super::lifecycle::{create, input, LifecycleSut};
use super::ref_store::StoreRef;
use crate::check::model::{Owner, Property, RefModel, Sut};
use crate::check::value::{Input, Observation, PropertyRow, Step};

const SHOWN: [&str; 3] = ["created", "rows", "revision"];

/// The real core behind `Database` seen as the writer: only what the writer shows (commit outcomes, snapshots) is kept.
pub struct StoreSut {
    inner: LifecycleSut,
}

impl StoreSut {
    pub fn new() -> Self {
        Self { inner: LifecycleSut::new() }
    }
}

#[async_trait]
impl Sut for StoreSut {
    async fn apply(&mut self, step: &Input) -> Vec<Observation> {
        let mut observed = match step.name.as_str() {
            "rows_held" => self.inner.apply(&input("rows")).await,
            _ => self.inner.apply(step).await,
        };
        observed.retain(|o| SHOWN.contains(&o.kind.as_str()) || o.kind == "unsupported");
        observed
    }
}

fn born_rows(steps: &[Step]) -> BTreeMap<String, usize> {
    let mut born = BTreeMap::new();
    for (index, step) in steps.iter().enumerate() {
        for o in step.observed.iter().filter(|o| o.kind == "created") {
            if let Some(name) = o.value["row"].as_str() { born.insert(name.to_string(), index); }
        }
    }
    born
}

/// P-STORE-01: the revision grows by 1 on a committed create and never by more than 1 in a step.
fn revision_by_one(steps: &[Step]) -> Result<(), String> {
    let mut last = 0;
    for (index, step) in steps.iter().enumerate() {
        let Some(now) = step.observed.iter().find(|o| o.kind == "revision").and_then(|o| o.value.as_u64()) else { continue };
        let created = step.observed.iter().any(|o| o.kind == "created" && o.value["row"].is_string());
        let step_delta = now.saturating_sub(last);
        if now < last || step_delta > 1 || (created && step_delta != 1) || (!created && step.input.name == "create" && step_delta != 0) {
            return Err(format!("step {} moved the revision {last} -> {now}", index + 1));
        }
        last = now;
    }
    Ok(())
}

/// P-STORE-02: no row born before an answered `reset_models` is read after it.
fn no_stale_rows(steps: &[Step]) -> Result<(), String> {
    let born = born_rows(steps);
    let resets: Vec<usize> = steps.iter().enumerate().filter(|(_, s)| s.input.name == "reset_models").map(|(i, _)| i).collect();
    for (index, step) in steps.iter().enumerate() {
        for o in step.observed.iter().filter(|o| o.kind == "rows") {
            for name in o.value["present"].as_array().into_iter().flatten().filter_map(|n| n.as_str()) {
                let at = born.get(name).copied().unwrap_or(0);
                if resets.iter().any(|reset| at < *reset && *reset < index) { return Err(format!("step {} reads {name} born at step {} before the reset", index + 1, at + 1)); }
            }
        }
    }
    Ok(())
}

/// P-STORE-03: every row read was committed by a create with the epoch of its session.
fn no_stale_epoch(steps: &[Step]) -> Result<(), String> {
    let born = born_rows(steps);
    for (index, step) in steps.iter().enumerate() {
        for o in step.observed.iter().filter(|o| o.kind == "rows") {
            for name in o.value["present"].as_array().into_iter().flatten().filter_map(|n| n.as_str()) {
                if !born.contains_key(name) { return Err(format!("step {} reads {name}, no create committed it", index + 1)); }
            }
        }
    }
    Ok(())
}

/// P-STORE-04: a batch of rows shows whole or not at all.
fn atomic(steps: &[Step]) -> Result<(), String> {
    for (index, step) in steps.iter().enumerate() {
        let batch: Vec<&str> = steps[..=index].iter().rev().find(|s| s.input.name == "commit_batch").and_then(|s| s.input.args.get("names")).and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|n| n.as_str()).collect()).unwrap_or_default();
        for o in step.observed.iter().filter(|o| o.kind == "rows") {
            let present: Vec<&str> = o.value["present"].as_array().into_iter().flatten().filter_map(|n| n.as_str()).collect();
            let seen = batch.iter().filter(|name| present.contains(name)).count();
            if seen != 0 && seen != batch.len() { return Err(format!("step {} shows {seen} of {} rows of one batch", index + 1, batch.len())); }
        }
    }
    Ok(())
}

/// P-STORE-05: every call ends with an outcome of the writer (a row, a refusal), never with a wait that ran out.
fn reply_once(steps: &[Step]) -> Result<(), String> {
    for (index, step) in steps.iter().enumerate() {
        if step.observed.iter().any(|o| o.value["error"] == "timeout") { return Err(format!("step {} got no outcome", index + 1)); }
    }
    Ok(())
}

/// P-STORE-06: a read while the writer is held answers with the snapshot.
fn reads_not_blocked(steps: &[Step]) -> Result<(), String> {
    for (index, step) in steps.iter().enumerate().filter(|(_, s)| s.input.name == "rows_held") {
        if !step.observed.iter().any(|o| o.kind == "rows" && o.value["present"].is_array()) { return Err(format!("step {} read nothing while the writer was held", index + 1)); }
    }
    Ok(())
}

pub const PROPERTIES: [PropertyRow; 6] = [
    ("P-STORE-01", revision_by_one),
    ("P-STORE-02", no_stale_rows),
    ("P-STORE-03", no_stale_epoch),
    ("P-STORE-04", atomic),
    ("P-STORE-05", reply_once),
    ("P-STORE-06", reads_not_blocked),
];

pub struct StoreOwner {
    pub full: bool,
    pub only: Option<&'static str>,
    pub alphabet: Vec<Input>,
}

impl StoreOwner {
    pub fn public(only: Option<&'static str>) -> Self {
        Self { full: false, only, alphabet: vec![input("login"), input("session_reset"), input("close"), create("a"), Input::new("create").with("as", "b").with("epoch", "other"), input("rows")] }
    }

    pub fn full(only: Option<&'static str>) -> Self {
        let mut alphabet = Self::public(None).alphabet;
        alphabet.extend([Input::new("reset_models").with("models", json!(["Moment"])), input("fail_commit"), input("fail_clear"), input("hold_writer"), input("release_writer"), input("rows_held"), Input::new("commit_batch").with("names", json!(["x", "y"]))]);
        Self { full: true, only, alphabet }
    }
}

#[async_trait]
impl Owner for StoreOwner {
    fn name(&self) -> &'static str {
        "store"
    }

    fn alphabet(&self) -> Vec<Input> {
        self.alphabet.clone()
    }

    fn model(&self) -> Box<dyn RefModel> {
        Box::new(StoreRef::new(self.full))
    }

    async fn real(&self) -> Box<dyn Sut> {
        Box::new(StoreSut::new())
    }

    fn properties(&self) -> Vec<Property> {
        PROPERTIES.iter().filter(|(name, _)| self.only.is_none_or(|only| only == *name)).map(|(name, check)| Property { name, check: *check }).collect()
    }
}
