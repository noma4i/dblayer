use super::shapes::{self, NETWORK_RETRIES, SERVER_RETRIES};
use super::window::{Key, Phase, Run};
use super::{Ending, Form, LibraryRef};
use crate::check::value::Observation;

/// What an answer input sends to the oldest request the mock holds.
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
            match reply {
                Reply::Page { rows, more } => self.page_answer(held.key, rows, more),
                Reply::Error(kind) => self.refusal(held.key, &kind),
            }
            return vec![shapes::replied(&held.id, &how)];
        }
        vec![shapes::error("no_request")]
    }

    fn page_answer(&mut self, key: Key, rows: u64, more: bool) {
        let Some(run) = self.run_of(key).take() else { return };
        self.land(key, run, rows, more);
    }

    fn refusal(&mut self, key: Key, kind: &str) {
        let class = match kind {
            "temporary" => Class::Network,
            "server" => Class::Server,
            _ => Class::Permanent,
        };
        let Some(mut run) = self.run_of(key).take() else { return };
        match class {
            Class::Network => run.network += 1,
            Class::Server => run.server += 1,
            Class::Permanent => {}
        }
        if run.network > 0 && run.server > 0 {
            self.mark("H11_mixed_refusal_classes");
        }
        let exhausted = class == Class::Permanent || run.network > NETWORK_RETRIES || run.server > SERVER_RETRIES;
        if !exhausted {
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

    /// The load ends with a refusal: the window is not fresh any more and every call waiting on it gets the error.
    /// A `load_more` that waited behind the load decides again (library.md section 4, `load_more` order, step 3).
    fn fail(&mut self, key: Key, kind: &'static str) {
        self.win(key).unfresh = true;
        let waiting: Vec<(u64, bool)> = self.calls.iter().filter(|call| call.key == Some(key)).map(|call| (call.id, call.queued)).collect();
        for (id, _) in waiting.iter().filter(|(_, queued)| !queued) {
            self.finish(*id, Ending::Failed(kind));
        }
        let queued: Vec<u64> = waiting.iter().filter(|(_, queued)| *queued).map(|(id, _)| *id).collect();
        let (cursor, pages) = {
            let window = self.win(key);
            (window.cursor.is_some(), window.pages)
        };
        let Some(first) = queued.first().copied() else { return };
        if !cursor {
            queued.iter().for_each(|id| self.finish(*id, Ending::More("no_more")));
            return;
        }
        if let Some(call) = self.calls.iter_mut().find(|call| call.id == first) {
            call.queued = false;
            call.target = Some(pages + 1);
        }
        self.start_run(key, pages + 1);
    }

    /// A page lands: page 1 (and the node) replaces the window and stamps it, a later page appends. Then the waiting calls end and the chain goes on.
    fn land(&mut self, key: Key, run: Run, rows: u64, more: bool) {
        let now = self.now;
        let window = self.win(key);
        match (key, run.page) {
            (Key::Node, _) => {
                window.members = u64::from(rows > 0);
                window.cursor = None;
                window.pages = 1;
                window.stamp(now);
            }
            (Key::Feed, 1) => {
                window.members = rows;
                window.cursor = more.then(|| format!("c{rows}"));
                window.pages = 1;
                window.stamp(now);
            }
            (Key::Feed, page) => {
                window.members += rows;
                window.cursor = more.then(|| format!("c{}", window.members));
                window.pages = page;
            }
        }
        let (cursor, pages) = (window.cursor.is_some(), window.pages);
        let waiting: Vec<(u64, Form, bool, Option<u32>)> = self.calls.iter().filter(|call| call.key == Some(key)).map(|call| (call.id, call.form, call.queued, call.target)).collect();
        let result = if cursor { "loaded_more" } else { "loaded_last" };
        for (id, form, queued, target) in &waiting {
            match form {
                Form::BeginLoad | Form::Refresh | Form::Node => self.finish(*id, Ending::Done),
                Form::LoadMore if !queued && (*target == Some(run.page) || run.page == 1) => self.finish(*id, Ending::More(result)),
                Form::LoadAll if !cursor => self.finish(*id, Ending::Done),
                Form::LoadMore | Form::LoadAll | Form::Nodes => {}
            }
        }
        let chain = waiting.iter().any(|(id, form, _, _)| *form == Form::LoadAll && self.calls.iter().any(|call| call.id == *id));
        let queued = waiting.iter().find(|(id, form, queued, _)| *form == Form::LoadMore && *queued && self.calls.iter().any(|call| call.id == *id)).map(|(id, ..)| *id);
        if !cursor {
            for (id, form, queued, _) in &waiting {
                if *form == Form::LoadMore && *queued {
                    self.finish(*id, Ending::More("no_more"));
                }
            }
            return;
        }
        if chain || queued.is_some() {
            if let Some(call) = self.calls.iter_mut().find(|call| Some(call.id) == queued) {
                call.queued = false;
                call.target = Some(pages + 1);
            }
            self.start_run(key, pages + 1);
        }
    }
}
