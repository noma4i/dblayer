use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Stamp {
    pub millis: u64,
    pub counter: u32,
    pub replica: [u8; 16],
}

pub struct HybridClock {
    replica: [u8; 16],
    last: Option<(u64, u32)>,
}

impl HybridClock {
    pub fn new(replica: [u8; 16], last: Option<Stamp>) -> Self {
        Self { replica, last: last.map(|stamp| (stamp.millis, stamp.counter)) }
    }

    pub fn observe(&mut self, remote: Stamp) {
        let observed = (remote.millis, remote.counter);
        self.last = Some(self.last.map_or(observed, |last| last.max(observed)));
    }

    pub fn tick(&mut self, wall_millis: u64) -> Option<Stamp> {
        let (millis, counter) = match self.last {
            None => (wall_millis, 0),
            Some((millis, _)) if wall_millis > millis => (wall_millis, 0),
            Some((millis, counter)) => match counter.checked_add(1) {
                Some(counter) => (millis, counter),
                None => (millis.checked_add(1)?, 0),
            },
        };
        self.last = Some((millis, counter));
        Some(Stamp { millis, counter, replica: self.replica })
    }
}
