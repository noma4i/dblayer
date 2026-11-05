#![allow(clippy::result_large_err)]
use std::fmt;

use async_trait::async_trait;

use super::value::{list, Input, Observation, Step, Trace};

/// The executable form of a spec table: one input in, the expected observations out.
pub trait RefModel: Send {
    fn step(&mut self, input: &Input) -> Vec<Observation>;
}

/// The real core behind its public API: runs one input and returns what a caller can observe.
/// An error is an observation (`error`), an input the runner cannot execute is the observation `unsupported`.
#[async_trait]
pub trait Sut: Send {
    async fn apply(&mut self, input: &Input) -> Vec<Observation>;
}

/// A property `P-<OWNER>-NN` over the trace so far (steps carry the observations).
#[derive(Clone, Copy)]
pub struct Property {
    pub name: &'static str,
    pub check: fn(&[Step]) -> Result<(), String>,
}

/// What an owner gives the framework: its input list, its model, a fresh real core and its properties.
#[async_trait]
pub trait Owner: Sync {
    fn name(&self) -> &'static str;
    fn alphabet(&self) -> Vec<Input>;
    fn model(&self) -> Box<dyn RefModel>;
    async fn real(&self) -> Box<dyn Sut>;
    fn properties(&self) -> Vec<Property>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum Fault {
    Observations,
    Property { name: &'static str, message: String },
}

/// Step number (from 1), input, expected, got.
#[derive(Clone, Debug, PartialEq)]
pub struct Divergence {
    pub step: usize,
    pub input: Input,
    pub expected: Vec<Observation>,
    pub got: Vec<Observation>,
    pub fault: Fault,
    pub inputs: Vec<Input>,
}

impl fmt::Display for Divergence {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "step {} input {} expected {} got {}", self.step, self.input, list(&self.expected), list(&self.got))?;
        if let Fault::Property { name, message } = &self.fault {
            write!(out, " property {name}: {message}")?;
        }
        Ok(())
    }
}

/// Runs the inputs through the model and the real core, compares after every input, checks the properties after every input.
/// Returns the trace with the expected observations.
pub async fn run(owner: &dyn Owner, name: &str, inputs: &[Input]) -> Result<Trace, Divergence> {
    let mut model = owner.model();
    let mut real = owner.real().await;
    let properties = owner.properties();
    let mut steps: Vec<Step> = Vec::new();
    for (index, input) in inputs.iter().enumerate() {
        let expected = model.step(input);
        let got = real.apply(input).await;
        let failure = |fault: Fault| Divergence { step: index + 1, input: input.clone(), expected: expected.clone(), got: got.clone(), fault, inputs: inputs.to_vec() };
        if expected != got {
            return Err(failure(Fault::Observations));
        }
        steps.push(Step { input: input.clone(), observed: expected.clone() });
        for property in &properties {
            if let Err(message) = (property.check)(&steps) {
                return Err(failure(Fault::Property { name: property.name, message }));
            }
        }
    }
    Ok(Trace { name: name.into(), steps })
}

/// Runs the model alone: the expected observations of the inputs.
pub fn expected_trace(owner: &dyn Owner, name: &str, inputs: &[Input]) -> Trace {
    let mut model = owner.model();
    let steps = inputs.iter().map(|input| Step { input: input.clone(), observed: model.step(input) }).collect();
    Trace { name: name.into(), steps }
}

/// Plays a recorded trace (from a corpus file) against a real core: the observations must equal the recorded ones.
pub async fn replay(real: &mut dyn Sut, trace: &Trace) -> Result<(), Divergence> {
    let inputs = trace.inputs();
    for (index, step) in trace.steps.iter().enumerate() {
        let got = real.apply(&step.input).await;
        if got != step.observed {
            return Err(Divergence { step: index + 1, input: step.input.clone(), expected: step.observed.clone(), got, fault: Fault::Observations, inputs });
        }
    }
    Ok(())
}
