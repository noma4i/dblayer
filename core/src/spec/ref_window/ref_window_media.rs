use super::*;

// The media chain of the `window` machine: the marker check, the tail, the full read and the confirm.

impl Window {
    /// The request that opens a load of `page`: a media window starts its first page with the check of its marker.
    pub(super) fn issue_effect(&mut self, request: u64, attempt: u32, page: u32) -> Effect {
        if !(self.media && page == 1) {
            self.chain = None;
            return Effect::Request { request, attempt, page };
        }
        let up_to = self.marker.map_or(0, |marker| marker.sequence);
        self.chain = Some(Chain3 { stage: Stage3::MarkerCheck { up_to }, cursor: None, rows: 0, latest: 0, base: 0 });
        Effect::Marker { request, attempt, up_to }
    }

    /// The request of the load in flight again: the one its phase asks for.
    pub(super) fn current_effect(&self, request: u64, attempt: u32, page: u32) -> Effect {
        match self.chain.map(|chain| chain.stage) {
            Some(Stage3::MarkerCheck { up_to } | Stage3::Confirm { up_to }) => Effect::Marker { request, attempt, up_to },
            Some(Stage3::Tail { after }) => Effect::Tail { request, attempt, after },
            Some(Stage3::Full) | None => Effect::Request { request, attempt, page },
        }
    }

    pub(super) fn chain_next(&mut self, request: u64, force: bool, chain: Chain3) -> Vec<Effect> {
        let attempt = self.attempt_id();
        self.chain = Some(chain);
        self.state = State::Loading { request, attempt, retry: false, page: 1, force, attempts: 0 };
        vec![self.current_effect(request, attempt, 1)]
    }

    /// The chain ended: its rows and its marker land in 1 batch, or only the marker when the rows stay.
    pub(super) fn land_chain(&mut self, request: u64, chain: Chain3, marker: Marker3, keep_rows: bool) -> Vec<Effect> {
        self.page_count = 1;
        self.landing_marker = Some((marker, if keep_rows { self.members } else { chain.base + chain.rows }));
        let cursor = if keep_rows { self.cursor } else { None };
        self.state = State::Landing { request, page: 1, cursor, rows: (keep_rows && self.has_rows) || chain.rows > 0 };
        vec![Effect::Commit { request, replace_all: false }]
    }

    /// Rows `win_media_marker_same`, `win_media_tail`, `win_media_full`, `win_media_confirmed`, `win_media_confirm_retry`, `win_media_confirm_failed`.
    pub(super) fn media_marker(&mut self, request: u64, attempt: u32, (force, attempts): (bool, u32), marker: Marker3) -> Vec<Effect> {
        self.answered_at = self.now;
        let Some(mut chain) = self.chain.take() else { return Vec::new() };
        match chain.stage {
            Stage3::MarkerCheck { up_to } => {
                chain.latest = marker.sequence;
                let prefix = self.marker.is_some_and(|saved| saved.revision == marker.revision && saved.count == marker.count && saved.sequence == up_to);
                if prefix && marker.sequence == up_to {
                    return self.land_chain(request, chain, marker, true);
                }
                if prefix && marker.sequence > up_to {
                    chain.stage = Stage3::Tail { after: up_to };
                    chain.base = self.members;
                } else {
                    chain.stage = Stage3::Full;
                }
                self.chain_next(request, force, chain)
            }
            Stage3::Confirm { .. } => {
                if marker.count == chain.base + chain.rows {
                    return self.land_chain(request, chain, Marker3 { sequence: chain.latest, ..marker }, false);
                }
                if attempts + 1 < Failure::Server.retry_limit() {
                    self.chain = Some(chain);
                    self.state = State::Loading { request, attempt, retry: true, page: 1, force, attempts: attempts + 1 };
                    return vec![Effect::ScheduleRetry { request }];
                }
                self.fail(false)
            }
            Stage3::Tail { .. } | Stage3::Full => self.fail(false),
        }
    }

    /// Rows `win_media_next`, `win_media_bad_cursor`, `win_media_confirm`.
    pub(super) fn media_page(&mut self, request: u64, force: bool, cursor: Option<u32>, rows: bool, more_without_cursor: bool) -> Vec<Effect> {
        self.answered_at = self.now;
        let Some(mut chain) = self.chain.take() else { return Vec::new() };
        if !matches!(chain.stage, Stage3::Tail { .. } | Stage3::Full) { return self.fail(false); }
        if rows { chain.rows += 1; }
        if cursor.is_none() && !more_without_cursor {
            chain.stage = Stage3::Confirm { up_to: chain.latest };
            chain.cursor = None;
        } else {
            match cursor {
                Some(next) if chain.cursor != Some(next) => chain.cursor = Some(next),
                _ => return self.fail(false),
            }
        }
        self.chain_next(request, force, chain)
    }
}
