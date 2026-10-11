use std::time::Duration;

type KeyOf<P> = Box<dyn Fn(&P) -> String + Send + Sync>;

pub struct Debounce<P> {
    pub interval: Duration,
    key_of: KeyOf<P>,
}

impl<P> Debounce<P> {
    pub fn new(interval: Duration, key_of: impl Fn(&P) -> String + Send + Sync + 'static) -> Self {
        Self { interval, key_of: Box::new(key_of) }
    }

    pub fn coalesce(&self, mut pending: Vec<P>, incoming: P) -> Vec<P> {
        let incoming_key = (self.key_of)(&incoming);
        match pending.iter().position(|item| (self.key_of)(item) == incoming_key) {
            Some(index) => pending[index] = incoming,
            None => pending.push(incoming),
        }
        pending
    }
}
