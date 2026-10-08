#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachFetch {
    None,
    FetchMissing,
    FetchStale,
    FetchInvalidated,
}

impl AttachFetch {
    pub const REFRESH: AttachFetch = AttachFetch::FetchStale;

    pub fn decide(has_window: bool, invalidated: bool, is_fresh: bool, is_in_flight: bool) -> Self {
        if is_in_flight {
            return AttachFetch::None;
        }
        if !has_window {
            return AttachFetch::FetchMissing;
        }
        if invalidated {
            return AttachFetch::FetchInvalidated;
        }
        if !is_fresh {
            return AttachFetch::FetchStale;
        }
        AttachFetch::None
    }

    pub fn makes_request(self) -> bool {
        self != AttachFetch::None
    }

    pub fn requests_without_reader(reader_count: usize, invalidated: bool) -> bool {
        reader_count > 0 && invalidated
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachResult {
    StartedObservation,
    JoinedObservation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetachResult {
    KeptObservation,
    StoppedObservation,
    UnknownObservation,
}
