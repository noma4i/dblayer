use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchVerdict {
    StartFetch,
    AwaitInFlight,
    SkipFresh,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FetchCoalescing {
    in_flight: HashSet<String>,
}

impl FetchCoalescing {
    pub fn request(&mut self, key: &str, is_stale: bool) -> FetchVerdict {
        if self.in_flight.contains(key) {
            return FetchVerdict::AwaitInFlight;
        }
        if !is_stale {
            return FetchVerdict::SkipFresh;
        }
        self.in_flight.insert(key.to_string());
        FetchVerdict::StartFetch
    }

    pub fn finish(&mut self, key: &str) {
        self.in_flight.remove(key);
    }

    pub fn in_flight(&self) -> &HashSet<String> {
        &self.in_flight
    }
}
