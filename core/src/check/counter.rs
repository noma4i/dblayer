use async_trait::async_trait;
use serde_json::json;

use super::model::{Owner, Property, RefModel, Sut};
use super::value::{Input, Observation, Step};

/// The example owner of the framework: a counter bound to an epoch.
/// `inc{epoch}` counts when the epoch is the current one and is dropped without an observation otherwise (R4).
/// `epoch_ended` reports the count of the ended epoch and starts the next epoch at 0.
pub fn inc(epoch: u64) -> Input {
    Input::new("inc").with("epoch", epoch)
}

pub fn epoch_ended() -> Input {
    Input::new("epoch_ended")
}

fn count(epoch: u64, n: u64) -> Observation {
    Observation::new("count", json!({ "epoch": epoch, "n": n }))
}

fn ended(epoch: u64, n: u64) -> Observation {
    Observation::new("ended", json!({ "epoch": epoch, "n": n }))
}

pub struct CounterRef {
    epoch: u64,
    n: u64,
}

impl CounterRef {
    pub fn new() -> Self {
        Self { epoch: 1, n: 0 }
    }
}

impl RefModel for CounterRef {
    fn step(&mut self, input: &Input) -> Vec<Observation> {
        match input.name.as_str() {
            "inc" if input.number("epoch") == Some(self.epoch) => {
                self.n += 1;
                vec![count(self.epoch, self.n)]
            }
            "epoch_ended" => {
                let out = vec![ended(self.epoch, self.n)];
                self.epoch += 1;
                self.n = 0;
                out
            }
            _ => Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Bug {
    /// A stale or future epoch is counted once the count is 2.
    CountsOtherEpochAtTwo,
}

/// The implementation under test: counts finished epochs and the open count, the model keeps the epoch number itself.
pub struct Counter {
    finished: u64,
    open: u64,
    bug: Option<Bug>,
}

impl Counter {
    pub fn new(bug: Option<Bug>) -> Self {
        Self { finished: 0, open: 0, bug }
    }
}

#[async_trait]
impl Sut for Counter {
    async fn apply(&mut self, input: &Input) -> Vec<Observation> {
        let current = self.finished + 1;
        match input.name.as_str() {
            "inc" => {
                let same = input.number("epoch") == Some(current);
                let at_two = self.open == 2;
                if same || (self.bug == Some(Bug::CountsOtherEpochAtTwo) && at_two) {
                    self.open += 1;
                    vec![count(current, self.open)]
                } else {
                    Vec::new()
                }
            }
            "epoch_ended" => {
                let out = vec![ended(current, self.open)];
                self.finished += 1;
                self.open = 0;
                out
            }
            _ => vec![Observation::new("unsupported", json!(input.name))],
        }
    }
}

fn field(observation: &Observation, key: &str) -> u64 {
    observation.value[key].as_u64().unwrap_or(0)
}

/// P-COUNTER-01 (epoch): after `ended` of epoch e no `count` of epoch e follows.
fn no_work_after_end(steps: &[Step]) -> Result<(), String> {
    let mut finished: Vec<u64> = Vec::new();
    for (index, step) in steps.iter().enumerate() {
        for observation in &step.observed {
            match observation.kind.as_str() {
                "ended" => finished.push(field(observation, "epoch")),
                "count" if finished.contains(&field(observation, "epoch")) => {
                    return Err(format!("step {} counts in the ended epoch {}", index + 1, field(observation, "epoch")));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// P-COUNTER-02 (data): inside an epoch every `count` is the previous one plus 1, and `ended` repeats the last count.
fn counts_step_by_one(steps: &[Step]) -> Result<(), String> {
    let (mut epoch, mut last) = (1u64, 0u64);
    for (index, step) in steps.iter().enumerate() {
        for observation in &step.observed {
            match observation.kind.as_str() {
                "count" if field(observation, "n") != last + 1 => return Err(format!("step {} counts {} after {}", index + 1, field(observation, "n"), last)),
                "count" => last += 1,
                "ended" if field(observation, "n") != last => return Err(format!("step {} ends epoch {} with {} after {}", index + 1, epoch, field(observation, "n"), last)),
                "ended" => {
                    epoch += 1;
                    last = 0;
                }
                _ => {}
            }
        }
    }
    Ok(())
}

pub struct CounterOwner {
    pub bug: Option<Bug>,
    pub extra: Option<Property>,
}

impl CounterOwner {
    pub fn new(bug: Option<Bug>) -> Self {
        Self { bug, extra: None }
    }
}

#[async_trait]
impl Owner for CounterOwner {
    fn name(&self) -> &'static str {
        "counter"
    }

    fn alphabet(&self) -> Vec<Input> {
        vec![inc(1), inc(2), epoch_ended()]
    }

    fn model(&self) -> Box<dyn RefModel> {
        Box::new(CounterRef::new())
    }

    async fn real(&self) -> Box<dyn Sut> {
        Box::new(Counter::new(self.bug))
    }

    fn properties(&self) -> Vec<Property> {
        let mut all = vec![
            Property { name: "P-COUNTER-01", check: no_work_after_end },
            Property { name: "P-COUNTER-02", check: counts_step_by_one },
        ];
        all.extend(self.extra);
        all
    }
}
