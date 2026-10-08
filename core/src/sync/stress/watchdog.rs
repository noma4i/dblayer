use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const LIMIT: Duration = Duration::from_secs(60);

pub struct Progress {
    started: Vec<AtomicU64>,
    labels: Vec<Mutex<String>>,
    ops: Vec<AtomicU64>,
    base: Instant,
    done: AtomicBool,
}

impl Progress {
    pub fn new(workers: usize) -> Arc<Self> {
        Arc::new(Self {
            started: (0..workers).map(|_| AtomicU64::new(0)).collect(),
            labels: (0..workers).map(|_| Mutex::new(String::new())).collect(),
            ops: (0..workers).map(|_| AtomicU64::new(0)).collect(),
            base: Instant::now(),
            done: AtomicBool::new(false),
        })
    }

    pub fn begin(&self, worker: usize, label: &str) {
        *self.labels[worker].lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = label.to_string();
        let millis = self.base.elapsed().as_millis() as u64 + 1;
        self.started[worker].store(millis, Ordering::SeqCst);
    }

    pub fn end(&self, worker: usize) {
        self.started[worker].store(0, Ordering::SeqCst);
        self.ops[worker].fetch_add(1, Ordering::SeqCst);
    }

    pub fn ops(&self) -> Vec<u64> {
        self.ops.iter().map(|count| count.load(Ordering::SeqCst)).collect()
    }

    pub fn finish(&self) {
        self.done.store(true, Ordering::SeqCst);
    }

    pub fn summary(&self) -> String {
        let now = self.base.elapsed().as_millis() as u64;
        (0..self.started.len())
            .map(|worker| {
                let started = self.started[worker].load(Ordering::SeqCst);
                let label = self.labels[worker].lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
                let state = if started == 0 { format!("idle after {label}") } else { format!("inside {label} for {} ms", now.saturating_sub(started)) };
                format!("worker {worker}: {} ops done, {state}", self.ops[worker].load(Ordering::SeqCst))
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn stuck(&self) -> bool {
        let now = self.base.elapsed().as_millis() as u64;
        self.started.iter().any(|started| {
            let started = started.load(Ordering::SeqCst);
            started != 0 && Duration::from_millis(now.saturating_sub(started)) > LIMIT
        })
    }
}

pub fn watch(progress: Arc<Progress>, reproduce: String) {
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_millis(500));
        if progress.done.load(Ordering::SeqCst) {
            return;
        }
        if progress.stuck() {
            eprintln!("STRESS HANG: a step ran longer than {} s, so a thread is blocked.\n{}\nreproduce: {reproduce}", LIMIT.as_secs(), progress.summary());
            std::process::exit(3);
        }
    });
}
