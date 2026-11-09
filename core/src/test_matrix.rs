//! The collector of the domain matrices: every cell runs, the table goes to `.orca-artifacts/matrix/<name>.tsv`, and the test fails once at the end with the whole table.
use std::fmt::Write as _;

/// The directory of the crate, read at run time: a test binary does not carry the path of the workspace.
pub(crate) fn crate_dir() -> std::path::PathBuf {
    std::env::var_os("CARGO_MANIFEST_DIR").map(std::path::PathBuf::from).or_else(|| std::env::current_dir().ok()).unwrap_or_default()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Verdict {
    Green,
    Red,
    Unclassified,
}

pub(crate) struct Cell {
    pub path: String,
    pub kind: String,
    pub input: String,
    pub expected: String,
    pub observed: String,
    pub verdict: Verdict,
}

pub(crate) struct Matrix {
    name: &'static str,
    cells: Vec<Cell>,
}

impl Matrix {
    pub fn new(name: &'static str) -> Self { Self { name, cells: Vec::new() } }

    pub fn unclassified(&mut self, path: &str, kind: &str, input: &str, why: &str) { self.push(path, kind, input, "", why, Verdict::Unclassified); }

    pub fn check(&mut self, path: &str, kind: &str, input: &str, expected: &str, observed: &str) {
        let verdict = if expected == observed { Verdict::Green } else { Verdict::Red };
        self.push(path, kind, input, expected, observed, verdict);
    }

    fn push(&mut self, path: &str, kind: &str, input: &str, expected: &str, observed: &str, verdict: Verdict) {
        let clean = |text: &str| text.replace(['\t', '\n'], " ");
        self.cells.push(Cell { path: clean(path), kind: clean(kind), input: clean(input), expected: clean(expected), observed: clean(observed), verdict });
    }

    fn count(&self, verdict: Verdict) -> usize { self.cells.iter().filter(|cell| cell.verdict == verdict).count() }

    /// Writes the table and fails once if a cell is red or unclassified.
    pub fn finish(self) {
        let mut table = String::from("path\tkind\tinput\texpected\tobserved\tverdict\n");
        for cell in &self.cells {
            let _ = writeln!(table, "{}\t{}\t{}\t{}\t{}\t{:?}", cell.path, cell.kind, cell.input, cell.expected, cell.observed, cell.verdict);
        }
        let directory = crate_dir().join("../.orca-artifacts/matrix");
        let written = std::fs::create_dir_all(&directory).and_then(|()| std::fs::write(directory.join(format!("{}.tsv", self.name)), &table));
        let (green, red, unclassified) = (self.count(Verdict::Green), self.count(Verdict::Red), self.count(Verdict::Unclassified));
        let summary = format!("matrix {}: rows {} green {green} red {red} unclassified {unclassified}; tsv {}", self.name, self.cells.len(), if written.is_ok() { "written" } else { "NOT written" });
        eprintln!("{summary}");
        assert!(red + unclassified == 0, "{summary}\n{table}");
    }
}

fn first_line(payload: &(dyn std::any::Any + Send)) -> String {
    let text = payload.downcast_ref::<String>().cloned().or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string())).unwrap_or_default();
    text.lines().next().unwrap_or("").chars().take(140).collect()
}

/// Matrix S on a twin: every start state x every input x every second input. The twin compares the owner with its reference after each step and panics on a divergence; a panic is a red cell with its first line.
pub(crate) type Starts<'a, S> = Vec<(&'a str, fn() -> S)>;

pub(crate) fn state_input_pairs<S, I: Clone>(matrix: &mut Matrix, reference: &str, starts: Starts<S>, alphabet: Vec<(&str, I)>, step: impl Fn(&mut S, I)) {
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for (start_name, start) in &starts {
        for (first_name, first) in &alphabet {
            for (second_name, second) in std::iter::once(("-", None)).chain(alphabet.iter().map(|(name, input)| (*name, Some(input.clone())))) {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut twin = start();
                    step(&mut twin, first.clone());
                    if let Some(second) = second { step(&mut twin, second); }
                }));
                let observed = match &outcome { Ok(()) => "owner and reference agree".to_string(), Err(payload) => first_line(payload.as_ref()) };
                matrix.check(start_name, reference, &format!("{first_name} then {second_name}"), "owner and reference agree", &observed);
            }
        }
    }
    std::panic::set_hook(hook);
}
