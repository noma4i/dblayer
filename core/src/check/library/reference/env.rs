use serde_json::{json, Value};

use super::shapes::{self, BACKOFF_CEILINGS};
use super::window::{Phase, Run};
use super::{Ending, LibraryRef, Target};
use crate::check::value::Observation;

fn not_open() -> Vec<Observation> {
    vec![shapes::error("not_open")]
}

/// library.md section 7 and the stand inputs of the environment.
impl LibraryRef {
    /// The stand opens with the network on, the clock goes back by the `clock_back` total, the saved windows stay (library.md section 7 `close` + `open`).
    pub(super) fn open(&mut self) -> Vec<Observation> {
        if self.live {
            return vec![shapes::error("already_open")];
        }
        self.now -= self.back;
        self.back = 0;
        self.live = true;
        self.online = true;
        vec![Observation::bare("opened")]
    }

    /// The session call of the stand: one request, answered at once, the identity is there after it.
    pub(super) fn login(&mut self) -> Vec<Observation> {
        if !self.live {
            return not_open();
        }
        if !self.online || self.identity {
            self.mark("login_outside_first_online");
            return Vec::new();
        }
        let id = format!("r{}", self.next_request);
        self.next_request += 1;
        self.identity = true;
        vec![shapes::request(&id), shapes::sent(&id, shapes::SESSION_OPERATION, Value::Null), shapes::replied(&id, "page"), shapes::session()]
    }

    /// Every waiting call ends with `kind` and every request the mock holds is cancelled.
    fn end_calls(&mut self, kind: &'static str) {
        let ids: Vec<u64> = self.calls.iter().map(|call| call.id).collect();
        for id in ids {
            self.finish(id, Ending::Failed(kind));
        }
        self.held.iter_mut().for_each(|held| held.live = false);
    }

    /// Waiting calls get `Closed`. The windows keep the saved fields and lose `unfresh` and the load.
    pub(super) fn close(&mut self) -> Vec<Observation> {
        if !self.live {
            return not_open();
        }
        self.end_calls("closed");
        for window in self.windows.values_mut() {
            window.load = None;
            window.unfresh = false;
        }
        let released: Vec<String> = self.held.drain(..).map(|held| held.id).collect();
        self.live = false;
        self.outcomes.push((u64::MAX, Observation::bare("closed")));
        if !released.is_empty() {
            self.outcomes.push((u64::MAX, Observation::new("released", json!({ "ids": released }))));
        }
        Vec::new()
    }

    /// Waiting calls get `NoSession`, the windows of the new epoch are empty.
    pub(super) fn session_reset(&mut self) -> Vec<Observation> {
        if !self.live {
            return not_open();
        }
        self.end_calls("no_session");
        self.windows.clear();
        self.identity = false;
        self.outcomes.push((u64::MAX, Observation::bare("reset")));
        Vec::new()
    }

    fn running(&self) -> Vec<Target> {
        let windows = self.windows.iter().filter(|(_, window)| window.load.is_some()).map(|(key, _)| Target::Window(*key));
        let nodes = self.calls.iter().filter(|call| call.run.is_some()).map(|call| Target::Nodes(call.id));
        windows.chain(nodes).collect()
    }

    /// `Online(false)`: running requests are cancelled and the loads wait. `Online(true)`: a waiting load starts.
    pub(super) fn set_online(&mut self, online: bool) -> Vec<Observation> {
        if !self.live {
            return not_open();
        }
        let was = self.online;
        self.online = online;
        for target in self.running() {
            if matches!(target, Target::Nodes(_)) && was != online {
                self.mark("nodes_across_network_change");
            }
            if was && !online {
                let Some(run) = self.run_of(target).take() else { continue };
                self.kill(&run);
                *self.run_of(target) = Some(Run { phase: Phase::Waiting, ..run });
            }
            if !was && online && matches!(self.run_of(target), Some(Run { phase: Phase::Waiting, .. })) {
                self.send(target);
            }
        }
        vec![Observation::new("online", json!(online))]
    }

    /// The clock moves. A retry pause that reaches the ceiling of its delay sends the request again (docs/server-sync.md "Retries").
    pub(super) fn tick(&mut self, ms: u64) -> Vec<Observation> {
        self.now += ms as i64;
        for target in self.running() {
            let Some(run) = self.run_of(target) else { continue };
            let retries = (run.network + run.server).max(1) as usize;
            let Phase::Backoff(elapsed) = &mut run.phase else { continue };
            *elapsed += ms;
            if *elapsed >= BACKOFF_CEILINGS[(retries - 1).min(BACKOFF_CEILINGS.len() - 1)] {
                self.send(target);
            }
        }
        Vec::new()
    }

    /// library.md section 4 "cancel by the own handle": the caller gets `Interrupted` at once, the last interest takes the request back and the window loses its freshness.
    pub(super) fn cancel(&mut self, target: &str) -> Vec<Observation> {
        if !self.live {
            return not_open();
        }
        let Some(&id) = self.handles.get(target) else { return vec![shapes::error("unknown_target")] };
        let Some(key) = self.calls.iter().find(|call| call.id == id).map(|call| call.key) else { return Vec::new() };
        self.finish(id, Ending::Failed("interrupted"));
        let Some(key) = key else { return Vec::new() };
        if self.calls.iter().any(|call| call.key == Some(key)) {
            return Vec::new();
        }
        if let Some(run) = self.run_of(Target::Window(key)).take() {
            self.kill(&run);
            self.win(key).unfresh = true;
        }
        Vec::new()
    }
}
