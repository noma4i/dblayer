use serde_json::{json, Value};

use super::landing::Reply;
use super::shapes;
use super::window::{Key, Outcome, Phase, Run};
use super::{Ending, Held, LibraryRef, Login, Owner, Sent};
use crate::check::value::Observation;

fn not_open() -> Vec<Observation> {
    vec![shapes::error("not_open")]
}

/// library.md section 7 and the stand inputs of the environment.
impl LibraryRef {
    /// The symbolic stand opens with the network unknown until the first platform message (network.md, D-402), the clock goes back by the `clock_back` total, the saved windows and the identity stay (library.md section 7 `close` + `open`).
    pub(super) fn open(&mut self) -> Vec<Observation> {
        if self.live {
            return vec![shapes::error("already_open")];
        }
        self.now -= self.back;
        self.back = 0;
        self.live = true;
        self.online = None;
        self.awaiting.clear();
        vec![Observation::bare("opened")]
    }

    /// The session request goes to the front of the requests the stand holds (it answers the session first).
    fn send_session(&mut self) {
        let id = self.provisional();
        self.held.push(Held { id: id.clone(), live: true, owner: Owner::Session });
        self.requests.push(Sent { id, operation: shapes::SESSION_OPERATION, variables: Value::Null });
        if let Some(login) = &mut self.login_call {
            login.sent = true;
            login.pause = None;
        }
    }

    /// A session call starts: its request goes out when the network is on.
    fn start_login(&mut self, call: u64, queued: Vec<u64>) {
        self.login_call = Some(Login { call: Some(call), sent: false, attempts: 0, pause: None, queued });
        if self.on() {
            self.send_session();
        }
    }

    /// The stand input `login`: the session call, then an answer to the oldest request the stand holds.
    /// Coordinator answer H3: with an identity the call ends with `AlreadyAuthenticated` at once, without a known network it waits for the network.
    pub(super) fn login(&mut self) -> Vec<Observation> {
        if !self.live {
            return not_open();
        }
        let mut shown = Vec::new();
        if self.identity {
            shown.push(shapes::error("session"));
        } else {
            let call = self.next_call;
            self.next_call += 1;
            if let Some(first) = &mut self.login_call {
                first.queued.push(call);
            } else {
                self.start_login(call, Vec::new());
                shown.extend(self.announce());
            }
        }
        if let Some(at) = self.held.iter().rposition(|held| held.owner == Owner::Session) {
            let session = self.held.remove(at);
            self.held.insert(0, session);
        }
        shown.extend(self.answer(Reply::Page { rows: 0, more: false }));
        shown
    }

    /// The session request is answered with the page body of the stand: the identity is there.
    /// Coordinator answer H14c: a temporary refusal repeats the same request by the session policy and the call waits, a permanent refusal ends the call with the error and leaves no identity.
    pub(super) fn session_answer(&mut self, reply: Reply) {
        let Some(login) = &mut self.login_call else { return };
        match reply {
            Reply::Page { .. } => {
                let (call, queued) = (login.call, std::mem::take(&mut login.queued));
                self.login_call = None;
                self.identity = true;
                self.outcomes.extend(call.map(|call| (call, shapes::session())));
                queued.into_iter().for_each(|id| self.outcomes.push((id, shapes::error("session"))));
            }
            Reply::Error(kind) if kind == "temporary" || kind == "server" => {
                login.attempts += 1;
                login.sent = true;
                login.pause = Some(0);
            }
            Reply::Error(_) => {
                let (call, mut queued) = (login.call, std::mem::take(&mut login.queued));
                self.login_call = None;
                self.outcomes.extend(call.map(|call| (call, shapes::error("graphql"))));
                if !queued.is_empty() {
                    let next = queued.remove(0);
                    self.start_login(next, queued);
                }
            }
        }
    }

    /// Every waiting call ends with `kind` and every request the mock holds is cancelled.
    fn end_calls(&mut self, kind: &'static str) {
        let ids: Vec<u64> = self.calls.iter().map(|call| call.id).collect();
        for id in ids {
            self.finish(id, Ending::Failed(kind));
        }
        if let Some(mut login) = self.login_call.take() {
            self.outcomes.extend(login.call.take().map(|call| (call, shapes::error(kind))));
            std::mem::take(&mut login.queued).into_iter().for_each(|id| self.outcomes.push((id, shapes::error(kind))));
            if kind == "closed" {
                (login.sent, login.pause) = (false, None);
                self.login_call = Some(login);
            }
        }
        self.held.iter_mut().for_each(|held| held.live = false);
    }

    /// Waiting calls get `Closed`. The windows keep the saved fields and lose `unfresh` and the load.
    pub(super) fn close(&mut self) -> Vec<Observation> {
        if !self.live {
            return not_open();
        }
        self.end_streams("closed");
        self.end_calls("closed");
        self.awaiting.clear();
        for window in self.windows.values_mut() {
            window.load = None;
            window.unfresh = false;
            window.outcome = Outcome::None;
            window.error = None;
            window.landed_run = false;
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
        self.end_streams("session_ended");
        self.end_calls("no_session");
        self.awaiting.clear();
        self.windows.clear();
        self.row = false;
        self.stale_models.clear();
        self.identity = false;
        self.outcomes.push((u64::MAX, Observation::bare("reset")));
        Vec::new()
    }

    /// Section 11.4 `set_online(false)` with a live connection: the base keeps the models of the live streams as not fresh. The windows in memory stay fresh until the network return.
    fn remember_subscribed_models(&mut self) {
        if !self.identity {
            return;
        }
        let subscribed: Vec<u8> = self.streams.values().filter(|stream| stream.live).filter_map(|stream| stream.reads.key()).map(Key::model).collect();
        self.stale_models.extend(subscribed);
    }

    /// The network return after such a break: the windows of the remembered models are not fresh, so the watched ones are reread.
    fn mark_remembered_models_unfresh(&mut self) {
        let stale = std::mem::take(&mut self.stale_models);
        for key in [Key::Feed, Key::Node, Key::Nodes, Key::Members] {
            if stale.contains(&key.model()) {
                self.win(key).unfresh = true;
            }
        }
    }

    fn running(&self) -> Vec<Key> {
        self.windows.iter().filter(|(_, window)| window.load.is_some()).map(|(key, _)| *key).collect()
    }

    /// `Online(false)`: running requests are cancelled and the loads wait. `Online(true)`: the waiting session call and the waiting loads start.
    pub(super) fn set_online(&mut self, online: bool) -> Vec<Observation> {
        if !self.live {
            return not_open();
        }
        let was_on = self.on();
        let first = self.online.is_none();
        self.online = Some(online);
        if was_on && !online {
            if let Some(login) = &mut self.login_call {
                login.sent = false;
                login.pause = None;
            }
            self.held.iter_mut().filter(|held| held.owner == Owner::Session).for_each(|held| held.live = false);
            for key in self.running() {
                let Some(run) = self.run_of(key).take() else { continue };
                self.kill(&run);
                *self.run_of(key) = Some(Run { phase: Phase::Waiting, ..run });
            }
            self.remember_subscribed_models();
        }
        if !was_on && online {
            self.mark_remembered_models_unfresh();
            let mut starting = usize::from(matches!(self.login_call, Some(Login { sent: false, .. })));
            if starting == 1 {
                self.send_session();
            }
            for key in self.running() {
                if matches!(self.run_of(key), Some(Run { phase: Phase::Waiting, .. })) {
                    starting += 1;
                    self.send(key);
                }
            }
            starting += self.reread_on_connect();
            if starting > 1 {
                self.mark("H16_order_of_loads_that_start_together");
            }
        }
        let mut shown = vec![Observation::new("online", json!(online))];
        if first {
            for (id, form, name, force) in std::mem::take(&mut self.awaiting) {
                self.calls.retain(|call| call.id != id);
                shown.extend(self.load_call(form, name, force));
            }
        }
        shown
    }

    /// The clock moves. A retry pause that reached its ceiling sends the request again.
    pub(super) fn tick(&mut self, ms: u64) -> Vec<Observation> {
        self.now += ms as i64;
        if let Some(login) = &mut self.login_call {
            if let Some(elapsed) = &mut login.pause {
                *elapsed += ms;
                if *elapsed >= shapes::pause_ceiling_ms(login.attempts) {
                    self.send_session();
                }
            }
        }
        let now = self.now;
        for key in [Key::Feed, Key::Members, Key::Node] {
            let window = self.win(key);
            if window.gap_due && window.load.is_none() && !window.in_gap(now) {
                window.gap_due = false;
                self.start_run(key, 1);
            }
        }
        for key in self.running() {
            let Some(run) = self.run_of(key) else { continue };
            let ceiling = shapes::pause_ceiling_ms(run.attempts);
            let Phase::Backoff(elapsed) = &mut run.phase else { continue };
            *elapsed += ms;
            if *elapsed >= ceiling {
                self.send(key);
            }
        }
        Vec::new()
    }

    /// library.md section 4 "cancel by the own handle": the caller gets `Interrupted` at once, the last interest takes the request back and the window loses its freshness.
    pub(super) fn cancel(&mut self, target: &str) -> Vec<Observation> {
        if !self.live {
            return not_open();
        }
        let Some(&id) = self.handles.get(target) else {
            if !self.streams.contains_key(target) {
                return vec![shapes::error("unknown_target")];
            }
            self.cancel_stream(target);
            return Vec::new();
        };
        let Some(key) = self.calls.iter().find(|call| call.id == id).map(|call| call.key) else { return Vec::new() };
        self.finish(id, Ending::Failed("interrupted"));
        let Some(key) = key else { return Vec::new() };
        self.leave(key);
        Vec::new()
    }
}
