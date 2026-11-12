//! The collector of the domain matrices: every cell runs, the table goes to `target/matrix/<name>.tsv`, and the test fails once at the end with the whole table.
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
        let directory = crate_dir().join("../target/matrix");
        let written = std::fs::create_dir_all(&directory).and_then(|()| std::fs::write(directory.join(format!("{}.tsv", self.name)), &table));
        let (green, red, unclassified) = (self.count(Verdict::Green), self.count(Verdict::Red), self.count(Verdict::Unclassified));
        let summary = format!("matrix {}: rows {} green {green} red {red} unclassified {unclassified}; tsv {}", self.name, self.cells.len(), if written.is_ok() { "written" } else { "NOT written" });
        eprintln!("{summary}");
        assert!(red + unclassified == 0, "{summary}\n{table}");
    }
}
