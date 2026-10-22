use serde_json::Value;

use crate::schema::MergeRule;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Hlc {
    pub ms: i64,
    pub count: u32,
    pub replica: String,
}

impl Hlc {
    pub fn new(ms: i64, count: u32, replica: &str) -> Self {
        Self { ms, count, replica: replica.to_string() }
    }
}

pub fn outranks(rule: Option<&MergeRule>, candidate: (&Value, &Hlc), held: (&Value, &Hlc)) -> bool {
    let by_rule = rule.map_or(std::cmp::Ordering::Equal, |rule| rule.rank(candidate.0, held.0));
    by_rule.then_with(|| candidate.1.cmp(held.1)).is_gt()
}
