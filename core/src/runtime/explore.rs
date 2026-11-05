use super::det::{DetScheduler, TraceEntry};

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub depth: usize,
    pub max_traces: usize,
}

#[derive(Clone, Debug)]
pub struct Violation {
    pub choices: Vec<usize>,
    pub trace: Vec<TraceEntry>,
    pub message: String,
}

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub traces: usize,
    pub violations: Vec<Violation>,
    pub incomplete: bool,
}

pub fn explore(build: impl Fn() -> DetScheduler, limits: Limits, mut check: impl FnMut(&DetScheduler) -> Result<(), String>) -> Report {
    let mut report = Report::default();
    walk(&build, limits, &mut check, build(), &mut Vec::new(), &mut report);
    report
}

fn walk(build: &dyn Fn() -> DetScheduler, limits: Limits, check: &mut dyn FnMut(&DetScheduler) -> Result<(), String>, at: DetScheduler, prefix: &mut Vec<usize>, report: &mut Report) {
    let moves = at.choices().len();
    if prefix.len() == limits.depth || moves == 0 {
        report.traces += 1;
        return;
    }
    for choice in 0..moves {
        if report.traces >= limits.max_traces {
            report.incomplete = true;
            return;
        }
        let mut next = build();
        prefix.iter().for_each(|earlier| { next.step_choice(*earlier); });
        next.step_choice(choice);
        prefix.push(choice);
        match check(&next) {
            Ok(()) => walk(build, limits, check, next, prefix, report),
            Err(message) => {
                report.traces += 1;
                report.violations.push(Violation { choices: prefix.clone(), trace: next.trace().to_vec(), message });
            }
        }
        prefix.pop();
    }
}
