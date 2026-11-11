use super::shapes::{self, NETWORK_SENDS, SERVER_SENDS};
use super::window::{Key, Outcome, Phase, Run};
use super::{Ending, Form, LibraryRef, Owner};
use crate::check::value::Observation;

/// What an answer input sends to the oldest request the mock holds.
#[derive(Clone)]
pub(super) enum Reply {
    Page { rows: u64, more: bool },
    Error(String),
}

/// How a refusal is repeated (library.md section 5, G6): a network failure and a temporary server failure by the policy, a permanent error never.
#[derive(Clone, Copy, PartialEq)]
enum Class {
    Network,
    Server,
    Permanent,
}

/// library.md section 5: the answer of the oldest request and what the waiting calls get from it.
impl LibraryRef {
    /// The stand answers the oldest request it still holds. A request the library took back is skipped.
    pub(super) fn answer(&mut self, reply: Reply) -> Vec<Observation> {
        let how = match &reply {
            Reply::Page { .. } => "page".to_string(),
            Reply::Error(kind) => kind.clone(),
        };
        while !self.held.is_empty() {
            let held = self.held.remove(0);
            if !held.live {
                continue;
            }
            match (held.owner, reply) {
                (Owner::Session, reply) => self.session_answer(reply),
                (Owner::Window(key), Reply::Page { rows, more }) => self.page_answer(key, rows, more),
                (Owner::Window(key), Reply::Error(kind)) => self.refusal(key, &kind),
            }
            return vec![shapes::replied(&held.id, &how)];
        }
        vec![shapes::error("no_request")]
    }

    fn page_answer(&mut self, key: Key, rows: u64, more: bool) {
        let Some(run) = self.run_of(key).take() else { return };
        self.land(key, run, rows, more);
    }

    /// Coordinator answers H11, H12: one counter of sends per load, the limit of the class of this refusal, a permanent refusal ends at once.
    fn refusal(&mut self, key: Key, kind: &str) {
        let class = match kind {
            "temporary" => Class::Network,
            "server" => Class::Server,
            _ => Class::Permanent,
        };
        let Some(mut run) = self.run_of(key).take() else { return };
        run.attempts += 1;
        let limit = match class {
            Class::Network => NETWORK_SENDS,
            Class::Server => SERVER_SENDS,
            Class::Permanent => 0,
        };
        if run.attempts < limit {
            run.phase = Phase::Backoff(0);
            *self.run_of(key) = Some(run);
            return;
        }
        let name = match class {
            Class::Network => "network",
            Class::Server => "http",
            Class::Permanent => "graphql",
        };
        self.fail(key, name);
    }

    /// The load ends with a refusal (H7): the window is not fresh any more and every call waiting on it, a queued `load_more` too, gets the same error.
    fn fail(&mut self, key: Key, kind: &'static str) {
        let window = self.win(key);
        window.unfresh = true;
        window.outcome = Some(Outcome::Failed);
        window.error = Some(kind);
        let waiting: Vec<u64> = self.calls.iter().filter(|call| call.key == Some(key)).map(|call| call.id).collect();
        for id in waiting {
            self.finish(id, Ending::Failed(kind));
        }
    }

    /// A page lands: page 1 (and the node) replaces the window and stamps it, a later page appends. Then the waiting calls end and the chain goes on.
    fn land(&mut self, key: Key, run: Run, rows: u64, more: bool) {
        let now = self.now;
        let window = self.win(key);
        match (key, run.page) {
            (Key::Nodes, _) => {
                let waiting: Vec<u64> = self.calls.iter().filter(|call| call.key == Some(Key::Nodes)).map(|call| call.id).collect();
                waiting.into_iter().for_each(|id| self.finish(id, Ending::Rows(rows)));
                return;
            }
            (Key::Node, _) => {
                window.members = u64::from(rows > 0);
                window.cursor = None;
                window.pages = 1;
                window.land_first(now);
            }
            (Key::Feed, 1) => {
                window.members = rows;
                window.cursor = more.then(|| format!("c{rows}"));
                window.pages = 1;
                window.land_first(now);
            }
            (Key::Feed, page) => {
                window.members += rows;
                window.cursor = more.then(|| format!("c{}", window.members));
                window.pages = page;
            }
        }
        let (cursor, pages) = (window.cursor.is_some(), window.pages);
        self.row |= rows > 0;
        let waiting: Vec<(u64, Form, bool, Option<u32>)> = self.calls.iter().filter(|call| call.key == Some(key)).map(|call| (call.id, call.form, call.queued, call.target)).collect();
        let result = if cursor { "loaded_more" } else { "loaded_last" };
        for (id, form, queued, target) in &waiting {
            match form {
                Form::BeginLoad | Form::Refresh | Form::Node => self.finish(*id, Ending::Done),
                Form::LoadMore if !queued && *target == Some(run.page) => self.finish(*id, Ending::More(result)),
                Form::LoadAll if !cursor => self.finish(*id, Ending::Done),
                Form::LoadMore | Form::LoadAll | Form::Nodes => {}
            }
        }
        let alive = |this: &Self, id: u64| this.calls.iter().any(|call| call.id == id);
        let chain = waiting.iter().any(|(id, form, ..)| *form == Form::LoadAll && alive(self, *id));
        let owed = waiting.iter().any(|(id, form, queued, target)| *form == Form::LoadMore && !queued && target.is_some_and(|page| page > pages) && alive(self, *id));
        let queued: Vec<u64> = waiting.iter().filter(|(id, form, queued, _)| *form == Form::LoadMore && *queued && alive(self, *id)).map(|(id, ..)| *id).collect();
        if !cursor {
            let rest: Vec<u64> = waiting.iter().filter(|(id, form, ..)| *form == Form::LoadMore && alive(self, *id)).map(|(id, ..)| *id).collect();
            rest.iter().for_each(|id| self.finish(*id, Ending::More("loaded_last")));
            return;
        }
        if chain || owed || !queued.is_empty() {
            if !owed {
                if let Some(call) = self.calls.iter_mut().find(|call| Some(call.id) == queued.first().copied()) {
                    call.queued = false;
                    call.target = Some(pages + 1);
                }
            }
            self.start_run(key, pages + 1);
        }
    }
}
