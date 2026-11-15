use super::shapes::{LIFETIME_MS, REQUEST_GAP_MS};

/// The windows O1 knows: the feed list, the transcode status node of the row `m1`, and the key of `nodes(["m1"])` (it has no window, only a load that calls with the same ids join).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) enum Key {
    Feed,
    Node,
    Nodes,
    Members,
}

impl Key {
    /// The model of the window: the feed and the transcode status node are rows of the moment, `nodes(["m1"])` of the container, the members of the user.
    pub(super) fn model(self) -> u8 {
        match self {
            Key::Feed | Key::Node => 0,
            Key::Nodes => 1,
            Key::Members => 2,
        }
    }

    /// The ids of the rows of a window: `m1..` for moments, `u1..` for the users of `container_members`.
    pub(super) fn row_prefix(self) -> &'static str {
        if self == Key::Members { "u" } else { "m" }
    }
}

/// Where a load is: its request is held by the mock, it waits out a retry pause, or it waits for the network.
#[derive(Clone, Debug)]
pub(super) enum Phase {
    Flight(String),
    Backoff(u64),
    Waiting,
}

/// One load of a window: the page and the sends made so far (one counter for both refusal classes).
#[derive(Clone, Debug)]
pub(super) struct Run {
    pub(super) page: u32,
    pub(super) phase: Phase,
    pub(super) attempts: u32,
}

/// library.md section 2 `outcome`: the result of the last load of the interest.
#[derive(Clone, Copy, Default, PartialEq, Debug)]
pub(super) enum Outcome {
    #[default]
    None,
    Fresh,
    Landed,
    Failed,
}

/// library.md section 2: the saved fields (`members`, `cursor`, `landed_at`, `fresh_until`), `unfresh` and the load.
#[derive(Clone, Default, Debug)]
pub(super) struct Window {
    pub(super) members: u64,
    pub(super) cursor: Option<String>,
    pub(super) pages: u32,
    pub(super) landed_at: Option<i64>,
    pub(super) fresh_until: i64,
    pub(super) unfresh: bool,
    pub(super) load: Option<Run>,
    pub(super) outcome: Outcome,
    pub(super) error: Option<&'static str>,
    pub(super) landed_run: bool,
    /// window.md P-WIN-37: a reread asked inside the request gap, sent when the clock reaches the end of the gap.
    pub(super) gap_due: bool,
}

impl Window {
    /// library.md section 3 `fresh`. `stale_since` is not here: O1 has no `Stale` input.
    pub(super) fn fresh(&self, now: i64) -> bool {
        let Some(landed) = self.landed_at else { return false };
        !self.unfresh && now < self.fresh_until && landed - now < self.fresh_until - landed
    }

    /// window.md P-WIN-37: the last answer landed less than 1 s ago, so the key does not go to the network.
    pub(super) fn in_gap(&self, now: i64) -> bool {
        self.landed_at.is_some_and(|landed| self.error.is_none() && now >= landed && now - landed < REQUEST_GAP_MS)
    }

    pub(super) fn stamp(&mut self, now: i64) {
        self.landed_at = Some(now);
        self.fresh_until = now + LIFETIME_MS;
        self.unfresh = false;
    }
}

impl Window {
    /// library.md section 3 `phase(window)` for a window with an interest: one function of the load, `outcome` and the rows.
    pub(super) fn phase(&self, rows: bool) -> &'static str {
        let loading = matches!(self.load, Some(Run { phase: Phase::Flight(_) | Phase::Backoff(_), .. }));
        match self.outcome {
            _ if loading => "loading",
            Outcome::None => "idle",
            Outcome::Fresh | Outcome::Landed => "loaded",
            Outcome::Failed if rows => "loaded",
            Outcome::Failed => "failed",
        }
    }

    /// An interest or a load enters a fresh window that has no outcome.
    pub(super) fn enter_fresh(&mut self) {
        if self.outcome == Outcome::None {
            self.outcome = Outcome::Fresh;
        }
    }

    /// library.md section 3 `loaded`: a landing of this run, or a saved window that is fresh.
    pub(super) fn loaded(&self, now: i64) -> bool {
        self.landed_run || (self.landed_at.is_some() && self.fresh(now))
    }

    /// The refusal `Offline` that needs no request (section 4 `refresh`, `load_more`): the window shows the error.
    pub(super) fn refuse_offline(&mut self) {
        self.outcome = Outcome::Failed;
        self.error = Some("offline");
    }

    pub(super) fn land_first(&mut self, now: i64) {
        self.stamp(now);
        self.outcome = Outcome::Landed;
        self.error = None;
        self.landed_run = true;
    }
}
