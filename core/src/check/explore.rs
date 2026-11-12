use std::path::{Path, PathBuf};

use super::corpus;
use super::model::{expected_trace, run, Divergence, DivergenceKind, Owner};
use super::value::{Input, Trace};

/// Every sequence of 1 to `depth` inputs of the alphabet, shorter first.
pub fn all_traces(alphabet: &[Input], depth: usize) -> Vec<Vec<Input>> {
    let mut all: Vec<Vec<Input>> = Vec::new();
    let mut level: Vec<Vec<Input>> = vec![Vec::new()];
    for _ in 0..depth {
        let mut next = Vec::new();
        for prefix in &level {
            for input in alphabet {
                let mut trace = prefix.clone();
                trace.push(input.clone());
                next.push(trace);
            }
        }
        all.extend(next.iter().cloned());
        level = next;
    }
    all
}

/// A long trace chosen by the seed (xorshift64): the same seed gives the same trace.
pub fn seeded(alphabet: &[Input], seed: u64, length: usize) -> Vec<Input> {
    let mut state = seed.max(1);
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            alphabet[(state % alphabet.len() as u64) as usize].clone()
        })
        .collect()
}

/// The first divergence over all traces to `depth`, then over the seeded long traces.
pub async fn find(owner: &dyn Owner, depth: usize, seeds: &[u64], length: usize) -> Option<Divergence> {
    let alphabet = owner.alphabet();
    for inputs in all_traces(&alphabet, depth) {
        if let Err(divergence) = run(owner, owner.name(), &inputs).await {
            return Some(*divergence);
        }
    }
    for seed in seeds {
        if let Err(divergence) = run(owner, owner.name(), &seeded(&alphabet, *seed, length)).await {
            return Some(*divergence);
        }
    }
    None
}

fn same_fault(a: &DivergenceKind, b: &DivergenceKind) -> bool {
    match (a, b) {
        (DivergenceKind::Property { name: a, .. }, DivergenceKind::Property { name: b, .. }) => a == b,
        _ => a == b,
    }
}

/// Removes inputs while the same fault stays: the result is a trace where no single input can go.
pub async fn minimize(owner: &dyn Owner, divergence: Divergence) -> Divergence {
    let mut current = divergence;
    current.inputs.truncate(current.step);
    loop {
        let mut shrunk = false;
        let mut index = 0;
        while index < current.inputs.len() {
            let mut candidate = current.inputs.clone();
            candidate.remove(index);
            match run(owner, owner.name(), &candidate).await {
                Err(smaller) if !candidate.is_empty() && same_fault(&smaller.fault, &current.fault) => {
                    let mut smaller = *smaller;
                    smaller.inputs.truncate(smaller.step);
                    current = smaller;
                    shrunk = true;
                },
                _ => index += 1,
            }
        }
        if !shrunk {
            return current;
        }
    }
}

/// Writes the trace of a divergence into a corpus directory: inputs with the observations the model expects, or the observations the core gave when the owner has no model.
pub fn save(owner: &dyn Owner, dir: &Path, divergence: &Divergence) -> Result<PathBuf, String> {
    let rule = match &divergence.fault {
        DivergenceKind::Property { name, .. } => format!("{}_", name.to_lowercase()),
        DivergenceKind::Observations => String::new(),
    };
    let name = format!("{}_{rule}step{}_{}", owner.name(), divergence.step, divergence.inputs.len());
    let trace = match owner.model() {
        Some(mut model) => expected_trace(model.as_mut(), &name, &divergence.inputs),
        None => Trace { name, steps: divergence.steps.clone() },
    };
    corpus::write(dir, &trace)
}
