use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use serde_json::Value;

use super::report::SendResult;

#[derive(Clone, Debug)]
pub(crate) struct FinishedOperation {
    pub result: SendResult,
    pub answer: Option<Value>,
    pub value: Option<crate::db::Row>,
}

#[derive(Debug)]
pub(crate) struct OutcomeTicket {
    result: Mutex<Option<FinishedOperation>>,
    changed: tokio::sync::watch::Sender<u64>,
}

impl OutcomeTicket {
    fn new() -> Self {
        let (changed, _) = tokio::sync::watch::channel(0);
        Self { result: Mutex::new(None), changed }
    }

    pub(crate) async fn result(&self) -> FinishedOperation {
        let mut changes = self.changed.subscribe();
        loop {
            changes.borrow_and_update();
            if let Some(result) = super::super::locked(&self.result).clone() { return result; }
            if changes.changed().await.is_err() { unreachable!(); }
        }
    }

    pub(crate) fn finish(&self, value: FinishedOperation) {
        *super::super::locked(&self.result) = Some(value);
        self.changed.send_modify(|version| *version += 1);
    }
}

#[derive(Default)]
pub(crate) struct Outcomes {
    tickets: BTreeMap<i64, Weak<OutcomeTicket>>,
}

impl Outcomes {
    pub(crate) fn claim(&mut self, sequence: i64) -> Arc<OutcomeTicket> {
        let ticket = Arc::new(OutcomeTicket::new());
        self.tickets.insert(sequence, Arc::downgrade(&ticket));
        ticket
    }

    pub(crate) fn take_ticket(&mut self, sequence: i64) -> Option<Arc<OutcomeTicket>> {
        self.tickets.remove(&sequence).and_then(|ticket| ticket.upgrade())
    }
}
