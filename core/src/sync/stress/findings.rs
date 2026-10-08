use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const LIMIT: usize = 200;
const PER_TAG: usize = 6;

#[derive(Clone, Default)]
pub struct Findings {
    list: Arc<Mutex<Vec<String>>>,
    tracing: Arc<AtomicBool>,
}

impl Findings {
    pub fn tracing(self, on: bool) -> Self {
        self.tracing.store(on, Ordering::SeqCst);
        self
    }

    pub fn trace(&self, line: impl FnOnce() -> String) {
        if self.tracing.load(Ordering::SeqCst) {
            eprintln!("trace {}", line());
        }
    }

    pub fn report(&self, invariant: &str, detail: impl AsRef<str>) {
        let entry = format!("[{invariant}] {}", detail.as_ref());
        let mut list = self.list.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let same_tag = list.iter().filter(|known| known.starts_with(&format!("[{invariant}]"))).count();
        if list.len() < LIMIT && same_tag < PER_TAG && !list.contains(&entry) {
            list.push(entry);
        }
    }

    pub fn all(&self) -> Vec<String> {
        self.list.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }
}
