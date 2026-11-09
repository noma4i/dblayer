use std::sync::OnceLock;
use std::time::Duration;

use rand_core::RngCore;

/// The one place of the retry rules: how many sends a policy allows and how long it waits before the next one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum RetryPolicy {
    Http,
    QueryNetwork,
    QueryServer,
    Outbox,
    Upload,
    Cable,
    Bootstrap,
}

static SEED: OnceLock<u64> = OnceLock::new();

/// The jitter seed of the process: random at the first use, before `open` too. Tests pass a seed to `delay_seeded`, or to the runtime with `set_jitter`.
pub fn process_seed() -> u64 { *SEED.get_or_init(|| rand_core::OsRng.next_u64()) }

fn mix(value: u64) -> u64 {
    let mut z = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl RetryPolicy {
    /// The number of sends the policy allows, the first one included; `None` waits for an event instead.
    pub const fn limit(self) -> Option<u32> {
        match self {
            Self::Http | Self::Outbox => Some(40),
            Self::Upload => Some(50),
            Self::QueryNetwork => Some(3),
            Self::QueryServer => Some(2),
            Self::Cable | Self::Bootstrap => None,
        }
    }

    const fn shape(self) -> (Duration, Duration, f64) {
        match self {
            Self::Upload => (Duration::from_secs(1), Duration::from_secs(15), 0.5),
            _ => (Duration::from_secs(1), Duration::from_secs(30), 0.2),
        }
    }

    /// Whether send number `attempt` (the first send is 1) may go out.
    pub fn allows(self, attempt: u32) -> bool { self.limit().is_none_or(|limit| attempt <= limit) }

    /// The wait after send number `attempt` (>= 1) failed, before send `attempt + 1`.
    pub fn delay(self, attempt: u32) -> Duration { self.delay_seeded(attempt, process_seed()) }

    pub fn delay_seeded(self, attempt: u32, seed: u64) -> Duration {
        let (base, cap, jitter) = self.shape();
        let ceiling = base.saturating_mul(2u32.saturating_pow(attempt.saturating_sub(1))).min(cap);
        let unit = (mix(seed ^ u64::from(attempt)) >> 11) as f64 / (1u64 << 53) as f64;
        ceiling.mul_f64(1.0 - jitter.min(1.0) * unit)
    }
}
