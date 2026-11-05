pub type Epoch = u64;

#[derive(Clone, Copy, Debug)]
pub enum Command {
    Start(Epoch),
    End(Epoch),
    Deposit { epoch: Option<Epoch>, amount: u32 },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Observed {
    pub total: u32,
    pub ended: Vec<Epoch>,
    pub dropped: u64,
}

#[derive(Default)]
pub struct RefOwner {
    epoch: Option<Epoch>,
    seen: Observed,
}

impl RefOwner {
    pub fn apply(&mut self, command: Command) -> Observed {
        match command {
            Command::Start(epoch) => self.epoch = Some(epoch),
            Command::End(epoch) if self.epoch == Some(epoch) => {
                self.seen.ended.push(epoch);
                self.epoch = None;
            }
            Command::End(_) => self.seen.dropped += 1,
            Command::Deposit { epoch: None, amount } => self.seen.total += amount,
            Command::Deposit { epoch: Some(epoch), amount } if self.epoch == Some(epoch) => self.seen.total += amount,
            Command::Deposit { .. } => self.seen.dropped += 1,
        }
        self.seen.clone()
    }
}
