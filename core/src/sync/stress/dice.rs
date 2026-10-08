const STEP: u64 = 0x9E37_79B9_7F4A_7C15;

#[derive(Debug, Clone)]
pub struct Dice {
    state: u64,
}

impl Dice {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn for_worker(seed: u64, worker: usize) -> Self {
        Self::new(seed ^ (worker as u64 + 1).wrapping_mul(STEP))
    }

    pub fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(STEP);
        let mut mixed = self.state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    }

    pub fn below(&mut self, limit: u64) -> u64 {
        if limit == 0 {
            return 0;
        }
        self.next() % limit
    }

    pub fn percent(&mut self, chance: u64) -> bool {
        self.below(100) < chance
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        (!items.is_empty()).then(|| &items[self.below(items.len() as u64) as usize])
    }
}
