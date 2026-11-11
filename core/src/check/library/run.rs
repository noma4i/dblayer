use std::path::{Path, PathBuf};

use super::sut::LibrarySut;
use crate::check::corpus;
use crate::check::model::{Property, Sut};
use crate::check::value::{Input, Step, Trace};

/// A program that broke a property: the rule, the step, the inputs up to it and the observations the core gave.
#[derive(Clone, Debug)]
pub struct BrokenRule {
    pub rule: &'static str,
    pub message: String,
    pub step: usize,
    pub steps: Vec<Step>,
}

impl BrokenRule {
    pub fn inputs(&self) -> Vec<Input> {
        self.steps.iter().map(|step| step.input.clone()).collect()
    }
}

impl std::fmt::Display for BrokenRule {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(out, "{} at step {}: {}\nprogram: {}", self.rule, self.step, self.message, self.inputs().iter().map(ToString::to_string).collect::<Vec<_>>().join(" ; "))
    }
}

/// Plays the inputs on a fresh core and checks every property after every input. No reference model: the properties read the observations only.
pub async fn run_program(inputs: &[Input], properties: &[Property]) -> Result<Trace, Box<BrokenRule>> {
    let mut sut = LibrarySut::default();
    let mut steps: Vec<Step> = Vec::new();
    let mut failure = None;
    for input in inputs {
        let observed = sut.apply(input).await;
        steps.push(Step { input: input.clone(), observed });
        if let Some(property) = properties.iter().find(|property| (property.check)(&steps).is_err()) {
            let message = (property.check)(&steps).err().unwrap_or_default();
            failure = Some(Box::new(BrokenRule { rule: property.name, message, step: steps.len(), steps: steps.clone() }));
            break;
        }
    }
    sut.finish().await;
    match failure {
        Some(violation) => Err(violation),
        None => Ok(Trace { name: "library".into(), steps }),
    }
}

/// Removes inputs after the first `keep` (the fixed prefix) while the same rule stays broken.
pub async fn minimize(violation: BrokenRule, keep: usize, properties: &[Property]) -> BrokenRule {
    let rule = violation.rule;
    let mut current = violation;
    loop {
        let mut shrunk = false;
        let inputs = current.inputs();
        for index in keep..inputs.len() {
            let mut candidate = inputs.clone();
            candidate.remove(index);
            if let Err(smaller) = run_program(&candidate, properties).await {
                if smaller.rule == rule {
                    current = *smaller;
                    shrunk = true;
                    break;
                }
            }
        }
        if !shrunk {
            return current;
        }
    }
}

/// Writes the program of a violation as `<dir>/<rule>_<tag>.jsonl`.
pub fn save(dir: &Path, violation: &BrokenRule, tag: &str) -> Result<PathBuf, String> {
    let name = format!("{}_{tag}", violation.rule.to_lowercase());
    corpus::write(dir, &Trace { name, steps: violation.steps.clone() })
}

/// The corpus directory of the programs the general run found.
pub fn corpus_dir() -> PathBuf {
    corpus::shared_dir().join("library")
}
