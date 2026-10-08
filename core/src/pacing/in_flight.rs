use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InFlightKeys {
    keys: HashSet<String>,
}

impl InFlightKeys {
    pub fn begin(&mut self, key: &str) -> bool {
        !key.is_empty() && self.keys.insert(key.to_string())
    }

    pub fn finish(&mut self, key: &str) {
        self.keys.remove(key);
    }

    pub fn is_in_flight(&self, key: &str) -> bool {
        self.keys.contains(key)
    }

    pub fn count(&self) -> usize {
        self.keys.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchVerdict {
    StartFetch,
    AwaitInFlight,
    SkipFresh,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FetchCoalescing {
    in_flight: HashSet<String>,
    transport_calls: usize,
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
        self.transport_calls += 1;
        FetchVerdict::StartFetch
    }

    pub fn finish(&mut self, key: &str) {
        self.in_flight.remove(key);
    }

    pub fn is_busy(&self, key: &str) -> bool {
        self.in_flight.contains(key)
    }

    pub fn in_flight(&self) -> &HashSet<String> {
        &self.in_flight
    }

    pub fn transport_calls(&self) -> usize {
        self.transport_calls
    }
}
