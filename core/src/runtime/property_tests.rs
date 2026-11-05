use super::{explore, DetScheduler, Limits, Violation};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exhaustive {
    Complete,
    Incomplete,
    Violated,
}

#[derive(Clone, Copy, Debug)]
pub struct Property {
    pub property: &'static str,
    pub domain: &'static str,
    pub cells: &'static str,
    pub depth: usize,
    pub max_traces: usize,
    pub seeds: u64,
    pub threaded_public_test: &'static str,
}

#[derive(Clone, Debug)]
pub struct Manifest {
    pub property: &'static str,
    pub domain: &'static str,
    pub cells: &'static str,
    pub depth: usize,
    pub exhaustive_result: Exhaustive,
    pub seeds: u64,
    pub threaded_public_test: &'static str,
    pub traces: usize,
    pub violation: Option<Violation>,
}

impl Manifest {
    pub fn line(&self) -> String {
        format!("{} | {} | {} | {} | {:?} | {} | {}", self.property, self.domain, self.cells, self.depth, self.exhaustive_result, self.seeds, self.threaded_public_test)
    }

    pub fn assert_complete(&self) {
        if let Some(violation) = &self.violation {
            panic!("{}: {} after choices {:?}\n{:#?}", self.line(), violation.message, violation.choices, violation.trace);
        }
        assert_eq!(self.exhaustive_result, Exhaustive::Complete, "{}: the walk stopped at max_traces after {} traces", self.line(), self.traces);
    }
}

impl Property {
    pub fn explore(self, build: impl Fn() -> DetScheduler, mut check: impl FnMut(&DetScheduler) -> Result<(), String>) -> Manifest {
        let report = explore(&build, Limits { depth: self.depth, max_traces: self.max_traces }, &mut check);
        let mut violation = report.violations.into_iter().next();
        for seed in 0..self.seeds {
            if violation.is_some() {
                break;
            }
            violation = seeded(&build, seed, &mut check);
        }
        let exhaustive_result = match (&violation, report.incomplete) {
            (Some(_), _) => Exhaustive::Violated,
            (None, true) => Exhaustive::Incomplete,
            (None, false) => Exhaustive::Complete,
        };
        Manifest { property: self.property, domain: self.domain, cells: self.cells, depth: self.depth, exhaustive_result, seeds: self.seeds, threaded_public_test: self.threaded_public_test, traces: report.traces, violation }
    }
}

fn seeded(build: &impl Fn() -> DetScheduler, seed: u64, check: &mut impl FnMut(&DetScheduler) -> Result<(), String>) -> Option<Violation> {
    let mut det = build();
    det.reseed(seed);
    let mut moves = 0usize;
    while det.step() {
        moves += 1;
        assert!(moves < 100_000, "a seeded trace does not end");
        if let Err(message) = check(&det) {
            return Some(Violation { choices: Vec::new(), trace: det.trace().to_vec(), message: format!("seed {seed}: {message}") });
        }
    }
    None
}
