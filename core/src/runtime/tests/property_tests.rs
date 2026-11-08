use super::explore::walk;
use super::{explore, DetScheduler, Limits, Report, Violation};

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
        let mut violation = report.violations.first().cloned();
        for seed in 0..self.seeds {
            if violation.is_some() {
                break;
            }
            violation = seeded(&build, seed, &mut check);
        }
        self.manifest(report, violation)
    }

    /// Same walk as `explore` for a pure check: subtrees and seeds run on their own threads, results join in choice and seed order.
    pub fn explore_parallel(self, build: impl Fn() -> DetScheduler + Sync, check: impl Fn(&DetScheduler) -> Result<(), String> + Sync) -> Manifest {
        let report = explore_parallel(&build, Limits { depth: self.depth, max_traces: self.max_traces }, &check);
        let mut violation = report.violations.first().cloned();
        if violation.is_none() {
            let found: std::sync::Mutex<Vec<Option<Violation>>> = std::sync::Mutex::new((0..self.seeds).map(|_| None).collect());
            std::thread::scope(|scope| {
                for seed in 0..self.seeds {
                    let (build, check, found) = (&build, &check, &found);
                    scope.spawn(move || {
                        let own = seeded(build, seed, &mut |det| check(det));
                        found.lock().unwrap_or_else(std::sync::PoisonError::into_inner)[seed as usize] = own;
                    });
                }
            });
            violation = found.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner).into_iter().flatten().next();
        }
        self.manifest(report, violation)
    }

    fn manifest(self, report: Report, violation: Option<Violation>) -> Manifest {
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

const FRONTIER_JOBS: usize = 64;

fn replay(build: &dyn Fn() -> DetScheduler, prefix: &[usize]) -> DetScheduler {
    let mut at = build();
    prefix.iter().for_each(|choice| { at.step_choice(*choice); });
    at
}

/// Walks the same tree as `explore` for a pure check: the first levels are expanded in place until
/// there are enough subtrees, the subtrees run on worker threads and their reports join in frontier order.
fn explore_parallel(build: &(dyn Fn() -> DetScheduler + Sync), limits: Limits, check: &(dyn Fn(&DetScheduler) -> Result<(), String> + Sync)) -> Report {
    let mut report = Report::default();
    let mut frontier: Vec<Vec<usize>> = vec![Vec::new()];
    while !frontier.is_empty() && frontier.len() < FRONTIER_JOBS {
        let mut deeper = Vec::new();
        for prefix in &frontier {
            let at = replay(build, prefix);
            let moves = at.choices().len();
            if prefix.len() == limits.depth || moves == 0 {
                report.traces += 1;
                continue;
            }
            for choice in 0..moves {
                let mut next = replay(build, prefix);
                next.step_choice(choice);
                let mut path = prefix.clone();
                path.push(choice);
                match check(&next) {
                    Ok(()) => deeper.push(path),
                    Err(message) => {
                        report.traces += 1;
                        report.violations.push(Violation { choices: path, trace: next.trace().to_vec(), message });
                    }
                }
            }
        }
        frontier = deeper;
        if !report.violations.is_empty() {
            return report;
        }
    }
    let next_job = std::sync::atomic::AtomicUsize::new(0);
    let done: std::sync::Mutex<Vec<Option<Report>>> = std::sync::Mutex::new(frontier.iter().map(|_| None).collect());
    let workers = std::thread::available_parallelism().map_or(4, |count| count.get()).min(frontier.len());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let job = next_job.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let Some(prefix) = frontier.get(job) else { return };
                let mut part = Report::default();
                let mut path = prefix.clone();
                walk(build, limits, &mut |det| check(det), replay(build, prefix), &mut path, &mut part);
                done.lock().unwrap_or_else(std::sync::PoisonError::into_inner)[job] = Some(part);
            });
        }
    });
    for part in done.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner).into_iter().flatten() {
        report.traces += part.traces;
        report.incomplete |= part.incomplete;
        report.violations.extend(part.violations);
    }
    report
}
