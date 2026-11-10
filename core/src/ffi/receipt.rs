use std::sync::Arc;

use super::wire::WireRow;

/// What a receipt waits on: the old stack, the owner Outbox or the owner Transfer.
#[derive(Clone)]
pub(super) enum Ticket {
    Outbox(crate::owners::write::outbox::Receipt),
    Transfer(crate::owners::write::outbox::Receipt),
}

#[derive(uniffi::Object)]
pub struct WireReceiptTicket {
    pub(super) ticket: Option<Ticket>,
    pub(super) owner: Arc<crate::owners::life::Life>,
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

pub(super) fn receipt_of(epoch: u64, model: String, operation: Option<i64>, revision: i64, optimistic_value: Option<WireRow>, ticket: Option<crate::owners::write::outbox::Receipt>, owner: Arc<crate::owners::life::Life>) -> WireOperationReceipt {
    held_receipt(epoch, model, operation, revision, optimistic_value, ticket.map(Ticket::Outbox), owner)
}

pub(super) fn receipt_of_transfer(epoch: u64, model: String, held: crate::owners::write::outbox::Receipt, optimistic_value: Option<WireRow>, owner: Arc<crate::owners::life::Life>) -> WireOperationReceipt {
    held_receipt(epoch, model, Some(i64::try_from(held.op_id).unwrap_or(i64::MAX)), held.revision, optimistic_value, Some(Ticket::Transfer(held)), owner)
}

fn held_receipt(epoch: u64, model: String, operation: Option<i64>, revision: i64, optimistic_value: Option<WireRow>, ticket: Option<Ticket>, owner: Arc<crate::owners::life::Life>) -> WireOperationReceipt {
    let held = WireReceiptTicket { ticket, owner, epoch, model: model.clone(), operation, revision, optimistic_value: optimistic_value.clone() };
    WireOperationReceipt { epoch, model, operation, revision, optimistic_value, ticket: Arc::new(held) }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, uniffi::Record)]
pub struct ItemId { pub value: String }

#[derive(Clone, Debug, uniffi::Record)]
pub struct WireItemReceipt { pub item: ItemId, pub optimistic_value: WireRow, pub receipt: WireOperationReceipt }

/// The receipts of the items of a batch, in the order of the items. Each item has its own outcome.
#[derive(Clone, Debug, uniffi::Record)]
pub struct WireBatchReceipt {
    pub epoch: u64,
    pub items: Vec<WireItemReceipt>,
}
