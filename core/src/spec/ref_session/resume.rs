use super::{Answer, BatchId, Fail, ReplyId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Resume(ReplyId),
    Committed(BatchId),
    Rejected(BatchId),
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Remove { batch: BatchId },
    Reply { id: ReplyId, answer: Result<Option<String>, Fail> },
    Drained,
}

pub struct Resume {
    answer: Option<Answer>,
    taking: Option<(BatchId, Vec<ReplyId>)>,
    next_batch: BatchId,
    closing: bool,
}

impl Resume {
    pub fn restored(answer: Answer) -> Self { Self { answer: Some(answer), taking: None, next_batch: 1, closing: false } }

    pub fn step(&mut self, input: Input) -> Vec<Effect> {
        match input {
            Input::Resume(id) => {
                if let Some((_, waiters)) = &mut self.taking { waiters.push(id); return Vec::new(); }
                if self.answer.is_none() { return vec![Effect::Reply { id, answer: Ok(None) }]; }
                let batch = self.next_batch;
                self.next_batch += 1;
                self.taking = Some((batch, vec![id]));
                vec![Effect::Remove { batch }]
            }
            Input::Committed(batch) if self.taking.as_ref().is_some_and(|(id, _)| *id == batch) => {
                let (_, waiters) = self.taking.take().unwrap();
                let answer = self.answer.take().unwrap();
                let mut effects: Vec<_> = waiters.into_iter().enumerate().map(|(index, id)| Effect::Reply { id, answer: if index == 0 { answer.clone().map(Some) } else { Ok(None) } }).collect();
                if self.closing { effects.push(Effect::Drained); }
                effects
            }
            Input::Rejected(batch) if self.taking.as_ref().is_some_and(|(id, _)| *id == batch) => {
                let (_, waiters) = self.taking.take().unwrap();
                let mut effects: Vec<_> = waiters.into_iter().map(|id| Effect::Reply { id, answer: Err(if self.closing { Fail::Closed } else { Fail::Storage }) }).collect();
                if self.closing { effects.push(Effect::Drained); }
                effects
            }
            Input::Closing => { self.closing = true; if self.taking.is_some() { Vec::new() } else { vec![Effect::Drained] } }
            _ => Vec::new(),
        }
    }
}
