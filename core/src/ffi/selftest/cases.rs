/// Collects the rows of one table test and fails once at the end with every mismatch.
pub(super) struct Cases {
    name: &'static str,
    rows: usize,
    mismatches: Vec<String>,
}

impl Cases {
    pub(super) fn new(name: &'static str) -> Self { Self { name, rows: 0, mismatches: Vec::new() } }

    pub(super) fn check(&mut self, case: &str, expected: &str, observed: &str) {
        self.rows += 1;
        if expected != observed { self.mismatches.push(format!("{case}: expected {expected}, observed {observed}")); }
    }

    pub(super) fn cell(&mut self, path: &str, kind: &str, input: &str, expected: &str, observed: &str) { self.check(&format!("{path} / {kind} / {input}"), expected, observed); }

    pub(super) fn finish(self) {
        assert!(self.mismatches.is_empty(), "{}: {} of {} cases differ:\n{}", self.name, self.mismatches.len(), self.rows, self.mismatches.join("\n"));
    }
}

pub(super) fn error_kind(error: &crate::ffi::DbError) -> &'static str {
    use crate::ffi::DbError as E;
    match error {
        E::NoSession { .. } => "NoSession",
        E::Security { .. } => "Security",
        E::Config { .. } => "Config",
        E::Invalid { .. } => "Invalid",
        E::NotFound { .. } => "NotFound",
        E::AlreadyExists { .. } => "AlreadyExists",
        E::Storage { .. } => "Storage",
        E::Offline { .. } => "Offline",
        E::Dropped { .. } => "Dropped",
        E::Unauthorized { .. } => "Unauthorized",
        E::InFlight { .. } => "InFlight",
        E::Runtime { .. } => "Runtime",
        E::Internal { .. } => "Internal",
        E::Copy { .. } => "Copy",
        E::Network { .. } => "Network",
        E::Interrupted { .. } => "Interrupted",
        E::Http { .. } => "Http",
        E::RequestTooLarge { .. } => "RequestTooLarge",
        E::ResponseTooLarge { .. } => "ResponseTooLarge",
        E::Graphql { .. } => "Graphql",
        E::Session { .. } => "Session",
        E::Closed { .. } => "Closed",
        E::Rejected { .. } => "Rejected",
        E::CorruptManifest { .. } => "CorruptManifest",
        E::NotOpen { .. } => "NotOpen",
    }
}
