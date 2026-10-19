use std::time::Duration;
use rand_core::RngCore;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Backoff {
    pub base: Duration,
    pub cap: Duration,
    pub jitter: f64,
    pub max_attempts: u32,
    pub seed: u64,
}

impl Backoff {
    pub const fn new(base: Duration, cap: Duration) -> Self {
        Self { base, cap, jitter: 0.0, max_attempts: u32::MAX, seed: 0 }
    }

    pub const fn with_jitter(self, jitter: f64, seed: u64) -> Self {
        Self { jitter, seed, ..self }
    }

    pub const fn limited(self, max_attempts: u32) -> Self {
        Self { max_attempts, ..self }
    }

    pub fn randomized(self) -> Self {
        Self { seed: rand_core::OsRng.next_u64(), ..self }
    }

    pub fn allows(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }

    pub fn delay(&self, attempt: u32) -> Duration {
        let ceiling = self.base.saturating_mul(2u32.saturating_pow(attempt)).min(self.cap);
        if self.jitter <= 0.0 {
            return ceiling;
        }
        let unit = (mix(self.seed ^ u64::from(attempt)) >> 11) as f64 / (1u64 << 53) as f64;
        ceiling.mul_f64(1.0 - self.jitter.min(1.0) * unit)
    }
}

fn mix(value: u64) -> u64 {
    let mut z = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
