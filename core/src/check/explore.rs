use std::path::{Path, PathBuf};

use super::corpus;
use super::model::{expected_trace, run, Divergence, Owner};
use super::value::Input;

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
            return Some(divergence);
        }
    }
    for seed in seeds {
        if let Err(divergence) = run(owner, owner.name(), &seeded(&alphabet, *seed, length)).await {
            return Some(divergence);
        }
    }
    None
}

/// Removes inputs while a divergence stays: the result is a trace where no single input can go.
pub async fn minimize(owner: &dyn Owner, divergence: Divergence) -> Divergence {
    let mut current = divergence;
    current.inputs.truncate(current.step);
    loop {
        let mut shrunk = false;
        for index in 0..current.inputs.len() {
            let mut candidate = current.inputs.clone();
            candidate.remove(index);
            if candidate.is_empty() {
                continue;
            }
            if let Err(smaller) = run(owner, owner.name(), &candidate).await {
                let mut smaller = smaller;
                smaller.inputs.truncate(smaller.step);
                current = smaller;
                shrunk = true;
                break;
            }
        }
        if !shrunk {
            return current;
        }
    }
}

/// Writes the trace of a divergence (inputs with the observations the model expects) into a corpus directory.
pub fn save(owner: &dyn Owner, dir: &Path, divergence: &Divergence) -> Result<PathBuf, String> {
    let name = format!("{}_step{}_{}", owner.name(), divergence.step, divergence.inputs.len());
    corpus::write(dir, &expected_trace(owner, &name, &divergence.inputs))
}
