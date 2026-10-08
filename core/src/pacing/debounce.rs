use std::time::Duration;

type KeyOf<P> = Box<dyn Fn(&P) -> String + Send + Sync>;
type Merge<P> = Box<dyn Fn(P, P) -> P + Send + Sync>;

pub struct Debounce<P> {
    pub interval: Duration,
    key_of: KeyOf<P>,
    merge: Option<Merge<P>>,
}

impl<P> Debounce<P> {
    pub fn new(interval: Duration, key_of: impl Fn(&P) -> String + Send + Sync + 'static) -> Self {
        Self { interval, key_of: Box::new(key_of), merge: None }
    }

    pub fn merging(mut self, merge: impl Fn(P, P) -> P + Send + Sync + 'static) -> Self {
        self.merge = Some(Box::new(merge));
        self
    }

    pub fn coalesce(&self, mut pending: Vec<P>, incoming: P) -> Vec<P> {
        let incoming_key = (self.key_of)(&incoming);
        let Some(index) = pending.iter().position(|item| (self.key_of)(item) == incoming_key) else {
            pending.push(incoming);
            return pending;
        };
        let previous = std::mem::replace(&mut pending[index], incoming);
        if let Some(merge) = &self.merge {
            let incoming = pending.remove(index);
            pending.insert(index, merge(previous, incoming));
        }
        pending
    }

    pub fn coalesce_all(&self, payloads: Vec<P>) -> Vec<P> {
        payloads.into_iter().fold(Vec::new(), |pending, payload| self.coalesce(pending, payload))
    }
}
