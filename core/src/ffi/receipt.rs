use std::sync::Arc;

use super::wire::WireRow;
use super::DbError;

/// What a receipt waits on: the old stack (uploads) or the owner Outbox.
#[derive(Clone)]
pub(super) enum Ticket {
    Sync(Arc<crate::sync::OutcomeTicket>),
    Outbox(crate::owners::write::outbox::Receipt),
}

#[derive(uniffi::Object)]
pub struct WireReceiptTicket {
    pub(super) ticket: Option<Ticket>,
    pub(super) streams: Arc<crate::ffi::legacy::Leases>,
    pub(super) epoch: u64,
    pub(super) model: String,
    pub(super) operation: Option<i64>,
    pub(super) revision: i64,
    pub(super) optimistic_value: Option<WireRow>,
}

impl std::fmt::Debug for WireReceiptTicket {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("WireReceiptTicket").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct WireOperationReceipt {
    pub epoch: u64,
    pub model: String,
    pub operation: Option<i64>,
    pub revision: i64,
    pub optimistic_value: Option<WireRow>,
    pub ticket: Arc<WireReceiptTicket>,
}

pub(super) fn receipt(epoch: u64, model: String, operation: Option<i64>, revision: i64, optimistic_value: Option<WireRow>, ticket: Option<Arc<crate::sync::OutcomeTicket>>, streams: Arc<crate::ffi::legacy::Leases>) -> WireOperationReceipt {
    held_receipt(epoch, model, operation, revision, optimistic_value, ticket.map(Ticket::Sync), streams)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn receipt_of(epoch: u64, model: String, operation: Option<i64>, revision: i64, optimistic_value: Option<WireRow>, ticket: Option<crate::owners::write::outbox::Receipt>, streams: Arc<crate::ffi::legacy::Leases>) -> WireOperationReceipt {
    held_receipt(epoch, model, operation, revision, optimistic_value, ticket.map(Ticket::Outbox), streams)
}

fn held_receipt(epoch: u64, model: String, operation: Option<i64>, revision: i64, optimistic_value: Option<WireRow>, ticket: Option<Ticket>, streams: Arc<crate::ffi::legacy::Leases>) -> WireOperationReceipt {
    let held = WireReceiptTicket { ticket, streams, epoch, model: model.clone(), operation, revision, optimistic_value: optimistic_value.clone() };
    WireOperationReceipt { epoch, model, operation, revision, optimistic_value, ticket: Arc::new(held) }
}

#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum WireOperationOutcome {
    Confirmed { value: Option<WireRow>, answer_json: Option<String> },
    Rejected { failure: super::wire::WireFailure },
    DeliveryUnknown,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, uniffi::Record)]
pub struct ItemId { pub value: String }

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireItemReceipt { pub item: ItemId, pub optimistic_value: WireRow }

#[derive(uniffi::Object)]
pub struct WireBatchTicket {
    pub(super) receipt: crate::sync::OperationReceipt,
    pub(super) model: String,
    pub(super) items: Vec<WireItemReceipt>,
}

impl std::fmt::Debug for WireBatchTicket {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("WireBatchTicket").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct WireBatchReceipt {
    pub epoch: u64,
    pub operation: i64,
    pub revision: i64,
    pub items: Vec<WireItemReceipt>,
    pub ticket: Arc<WireBatchTicket>,
}

#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum WireItemOutcome {
    Confirmed { item: ItemId, value: WireRow },
    Discarded { item: ItemId },
}

#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum WireBatchResult {
    Confirmed,
    Rejected { failure: super::wire::WireFailure },
    DeliveryUnknown,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireBatchOutcome { pub result: WireBatchResult, pub items: Vec<WireItemOutcome> }
