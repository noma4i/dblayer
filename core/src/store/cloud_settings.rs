use rusqlite::Connection;

use super::{ddl, session_state, CloudAttachment, CloudAttachmentMode, Store, StoreError};
use crate::owners::cloud::CloudScope;
use crate::runtime::Epoch;

pub(super) const CLOUD_ENABLED: &str = "cloud_enabled";

fn read(connection: &Connection) -> Result<Option<bool>, StoreError> {
    match ddl::manifest_read(connection, CLOUD_ENABLED)?.as_deref() {
        None => Ok(None),
        Some("0") => Ok(Some(false)),
        Some("1") => Ok(Some(true)),
        Some(_) => Err(StoreError::CorruptManifest(CLOUD_ENABLED.into())),
    }
}

impl Store {
    pub(crate) fn restore_cloud_capture(&self, epoch: Epoch, binding: &str, application_key: Option<&str>) -> Result<Option<CloudScope>, StoreError> {
        let mut inner = self.lock();
        if inner.cloud_scope.is_some() || ddl::manifest_read(&inner.connection, "cloud_capture_binding")?.as_deref() != Some(binding) { return Ok(None); }
        let Some(domain) = ddl::manifest_read(&inner.connection, "cloud_domain")? else { return Ok(None); };
        if let Some(key) = application_key {
            let session = session_state::session(&inner.connection)?;
            let credential = ddl::manifest_read(&inner.connection, "cloud_application_credential")?;
            let stored_key = ddl::manifest_read(&inner.connection, "cloud_application_key")?;
            if stored_key.as_deref() != Some(key) || !session.is_some_and(|session| credential.as_deref() == Some(session.credential_digest.as_str())) {
                return Ok(None);
            }
        }
        let transaction = inner.connection.unchecked_transaction()?;
        super::cloud::enable(&transaction)?;
        transaction.commit()?;
        let scope = CloudScope { epoch, generation: 0, domain, provider_fingerprint: String::new() };
        inner.cloud_scope = Some(CloudAttachment { scope: scope.clone(), mode: CloudAttachmentMode::Offline });
        Ok(Some(scope))
    }

    pub(crate) fn cloud_status_scope(&self) -> Option<CloudScope> {
        self.lock().cloud_scope.as_ref().filter(|attachment| attachment.mode != CloudAttachmentMode::Erased).map(|attachment| attachment.scope.clone())
    }

    pub(crate) fn stored_cloud_enabled(&self) -> Result<Option<bool>, StoreError> {
        read(&self.lock().connection)
    }

    pub(crate) fn write_cloud_enabled(&self, enabled: bool) -> Result<(), StoreError> {
        let inner = self.lock();
        if read(&inner.connection)? == Some(enabled) { return Ok(()); }
        ddl::manifest_write(&inner.connection, CLOUD_ENABLED, if enabled { "1" } else { "0" })?;
        Ok(())
    }
}
