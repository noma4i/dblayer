use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use serde_json::Value;

use super::report::SendResult;

#[derive(Clone, Debug)]
pub(crate) struct FinishedOperation {
    pub result: SendResult,
    pub answer: Option<Value>,
    pub value: Option<crate::db::Row>,
    pub items: Vec<(String, crate::db::Row)>,
    pub discarded: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct OutcomeTicket {
    result: tokio::sync::watch::Sender<Option<FinishedOperation>>,
    discarded: Mutex<std::collections::BTreeSet<String>>,
}

impl OutcomeTicket {
    fn new() -> Self {
        Self { result: tokio::sync::watch::Sender::new(None), discarded: Mutex::new(Default::default()) }
    }

    pub(crate) async fn result(&self) -> FinishedOperation {
        let mut changes = self.result.subscribe();
        let finished = changes.wait_for(Option::is_some).await.ok().and_then(|finished| finished.clone());
        finished.unwrap_or(FinishedOperation { result: SendResult::Discarded, answer: None, value: None, items: Vec::new(), discarded: Vec::new() })
    }

    #[cfg(test)]
    pub(crate) fn finished(&self) -> Option<FinishedOperation> {
        self.result.borrow().clone()
    }

    pub(crate) fn discard_item(&self, item: String) {
        super::super::locked(&self.discarded).insert(item);
    }

    pub(crate) fn finish(&self, mut value: FinishedOperation) {
        value.discarded = super::super::locked(&self.discarded).iter().cloned().collect();
        crate::db::lock::effect(|| { self.result.send_replace(Some(value)); });
    }
}

#[derive(Default)]
pub(crate) struct Outcomes {
    tickets: BTreeMap<i64, Weak<OutcomeTicket>>,
    sending: std::collections::BTreeSet<i64>,
}

impl Outcomes {
    /// The ticket of an operation: a retry of upload items keeps the ticket the upload call returned.
    pub(crate) fn claim(&mut self, sequence: i64) -> Arc<OutcomeTicket> {
        if let Some(ticket) = self.ticket(sequence) { return ticket; }
        let ticket = Arc::new(OutcomeTicket::new());
        self.tickets.insert(sequence, Arc::downgrade(&ticket));
        ticket
    }

    pub(crate) fn take_ticket(&mut self, sequence: i64) -> Option<Arc<OutcomeTicket>> {
        self.tickets.remove(&sequence).and_then(|ticket| ticket.upgrade())
    }

    pub(crate) fn ticket(&self, sequence: i64) -> Option<Arc<OutcomeTicket>> {
        self.tickets.get(&sequence).and_then(Weak::upgrade)
    }

    pub(crate) fn begin_send(&mut self, sequence: i64) {
        self.sending.insert(sequence);
    }

    pub(crate) fn end_send(&mut self, sequence: i64) {
        self.sending.remove(&sequence);
    }

    /// Operations that a commit ended outside a send: the send settles its own operation with the answer it read.
    pub(crate) fn committed(&mut self, result: &crate::commit::CommitResult) -> Vec<(Arc<OutcomeTicket>, SendResult)> {
        for (sequence, item) in &result.discarded_items {
            if let Some(ticket) = self.ticket(*sequence) { ticket.discard_item(item.clone()); }
        }
        let uploads = result.ended_uploads.iter().map(|(sequence, phase, error)| (*sequence, upload_result(*phase, error.as_ref())));
        let operations = result.queue_operations.iter().filter_map(|(sequence, entry)| ended_result(entry.as_ref()).map(|ended| (*sequence, ended)));
        let mut ended = Vec::new();
        for (sequence, result) in uploads.chain(operations) {
            if self.sending.contains(&sequence) { continue; }
            if let Some(ticket) = self.take_ticket(sequence) { ended.push((ticket, result)); }
        }
        ended
    }

    pub(crate) fn reset(&mut self, kept: &[crate::operation::QueueOperation]) -> Vec<(Arc<OutcomeTicket>, SendResult)> {
        let kept: std::collections::BTreeSet<i64> = kept.iter().map(|entry| entry.operation.id.0).collect();
        let gone: Vec<i64> = self.tickets.keys().copied().filter(|sequence| !kept.contains(sequence) && !self.sending.contains(sequence)).collect();
        gone.into_iter().filter_map(|sequence| self.take_ticket(sequence)).map(|ticket| (ticket, SendResult::Discarded)).collect()
    }
}

fn refused(error: Option<&crate::syncstate::OperationFailure>) -> SendResult {
    SendResult::Rejected(error.cloned().unwrap_or_else(|| crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::Rejected, "the operation failed")))
}

fn ended_result(entry: Option<&crate::operation::QueueOperation>) -> Option<SendResult> {
    let Some(entry) = entry else { return Some(SendResult::Discarded) };
    match entry.operation.status {
        crate::operation::OperationStatus::Failed => Some(refused(entry.operation.error.as_ref())),
        crate::operation::OperationStatus::DeliveryUnknown => Some(SendResult::DeliveryUnknown),
        _ => None,
    }
}

fn upload_result(phase: crate::upload::UploadStatus, error: Option<&crate::syncstate::OperationFailure>) -> SendResult {
    if phase == crate::upload::UploadStatus::DeliveryUnknown { SendResult::DeliveryUnknown } else { refused(error) }
}
