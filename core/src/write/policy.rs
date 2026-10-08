use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::path::Path;
use crate::commit::WriteOrigin;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyPolicy {
    Server,
    Continuity,
    NonEmpty,
    Positive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Condition {
    NewerBy(Path),
    Tuple(Vec<Path>),
    NonEmpty,
    Present(Path),
    Equal(Path),
    Ladder(Path, Vec<Vec<String>>),
    All(Vec<Condition>),
    Any(Vec<Condition>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WritePolicy {
    Server,
    Local,
    Continuity,
    Snapshot,
    Monotonic(Condition),
    Keys(BTreeMap<Path, KeyPolicy>, Box<WritePolicy>),
}

impl WritePolicy {
    pub fn default_policy() -> WritePolicy {
        WritePolicy::Server
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteGroup {
    pub fields: Vec<Path>,
    pub policy: Vec<WritePolicy>,
    pub origins: Option<Vec<WriteOrigin>>,
}

impl WriteGroup {
    pub fn new(fields: Vec<Path>, policy: Vec<WritePolicy>, origins: Option<Vec<WriteOrigin>>) -> Self {
        Self { fields, policy, origins }
    }
}

pub fn applies_on_replace(policy: &WritePolicy) -> bool {
    !matches!(policy, WritePolicy::Server | WritePolicy::Monotonic(_))
}

pub fn default_origins(policy: &WritePolicy) -> Vec<WriteOrigin> {
    if matches!(policy, WritePolicy::Monotonic(_)) {
        return vec![WriteOrigin::Snapshot, WriteOrigin::Event, WriteOrigin::Response];
    }
    WriteOrigin::ALL.to_vec()
}

pub fn allowed_origins_for_monotonic(requested: &[WriteOrigin]) -> Vec<WriteOrigin> {
    let allowed = [WriteOrigin::Snapshot, WriteOrigin::Event, WriteOrigin::Response, WriteOrigin::Patch];
    requested.iter().copied().filter(|origin| allowed.contains(origin)).collect()
}

pub fn condition_paths(condition: &Condition) -> Vec<Path> {
    match condition {
        Condition::NewerBy(path) | Condition::Present(path) | Condition::Equal(path) | Condition::Ladder(path, _) => vec![path.clone()],
        Condition::Tuple(paths) => paths.clone(),
        Condition::NonEmpty => Vec::new(),
        Condition::All(conditions) | Condition::Any(conditions) => conditions.iter().flat_map(condition_paths).collect(),
    }
}

pub fn first_accepted_policy<'a>(group: &'a WriteGroup, rejected: &BTreeSet<usize>) -> Option<&'a WritePolicy> {
    group.policy.iter().enumerate().find(|(index, _)| !rejected.contains(index)).map(|(_, policy)| policy)
}
