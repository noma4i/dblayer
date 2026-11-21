use super::database::id;
use super::upload_wire::{WireStagedUpload, WireUploadRequest};
use super::{Database, DbError};
use crate::owners::store::IntentEffects;
use crate::owners::write::transfer::TransferAnswer;

#[uniffi::export]
impl Database {
    pub async fn stage_on_row(&self, epoch: u64, model: String, row_id: String, action: String, request: WireUploadRequest) -> Result<WireStagedUpload, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.account_call(epoch)?;
        let row = this_db.resolved(epoch, &model, id(&row_id)?).await?;
        let (name, target) = (model.clone(), row.clone());
        this_db.read_in_epoch(epoch, move |database| super::write::row_present(database, &name, &target)).await?;
        let file = this_db.offload(move || request.into_file()).await?;
        let single = this_db.single_of(&model, &action, &file, true)?;
        if single.source.is_none() { return Err(DbError::Invalid { detail: "a staged upload needs a file".into() }); }
        let shown = this_db.measured_file(&single.declaration, &file).await;
        let mut item = this_db.single_upload((&model, row.as_str(), &action), single, &file, IntentEffects::default())?;
        item.held = true;
        item.on_row = true;
        item.dimensions = Some(shown).filter(|shown| !shown.is_empty());
        let waiter = this_db.life.accept_upload(epoch, item);
        let TransferAnswer::Receipt(held) = this_db.transfer_answer(waiter, "stage").await? else { return Err(DbError::Internal { detail: "the owner answered a stage with no receipt".into() }) };
        Ok(WireStagedUpload { row_id: row.to_string(), receipt: this_db.transfer_receipt(epoch, model, held, None) })
    }).await? }
}
