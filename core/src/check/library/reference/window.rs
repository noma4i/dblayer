use super::shapes::LIFETIME_MS;

/// The windows O1 knows: the feed list and the transcode status node of the row `m1`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) enum Key {
    Feed,
    Node,
}

/// Where a load is: its request is held by the mock, it waits out a retry pause, or it waits for the network.
#[derive(Clone, Debug)]
pub(super) enum Phase {
    Flight(String),
    Backoff(u64),
    Waiting,
}

/// One load of a window: the page and the failures it met so far.
#[derive(Clone, Debug)]
pub(super) struct Run {
    pub(super) page: u32,
    pub(super) phase: Phase,
    pub(super) network: u32,
    pub(super) server: u32,
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
}

impl Window {
    /// library.md section 3 `fresh`. `stale_since` is not here: O1 has no `Stale` input.
    pub(super) fn fresh(&self, now: i64) -> bool {
        let Some(landed) = self.landed_at else { return false };
        !self.unfresh && now < self.fresh_until && landed - now < self.fresh_until - landed
    }

    pub(super) fn stamp(&mut self, now: i64) {
        self.landed_at = Some(now);
        self.fresh_until = now + LIFETIME_MS;
        self.unfresh = false;
    }
}
