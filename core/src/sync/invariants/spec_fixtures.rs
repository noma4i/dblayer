use serde_json::json;

use super::super::rig::*;
use super::super::SyncConfig;
use crate::db::Db;
use crate::transport::MockTransport;
use crate::schema::{ActionDeclaration, AssociationKind, AssociationSpec, Dependent, EphemeralSpec, FieldSpec, ModelSpec, RootShape};
use crate::value::ScalarKind;

pub fn specs_with_references() -> Vec<ModelSpec> {
    let mut moments = moments_spec();
    moments.fields.push(FieldSpec::file("media").optional());
    moments.ephemeral.push(EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false))).resetting_after("5.seconds"));
    let mut notes = ModelSpec::new(
        "notes",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("momentId", ScalarKind::Id).indexed(),
            FieldSpec::array("momentIds").optional(),
            FieldSpec::scalar("text", ScalarKind::Str).optional(),
        ],
    );
    notes.associations.push(AssociationSpec::new("moment", AssociationKind::BelongsTo, "moments", "momentId"));
    notes.associations.push(AssociationSpec::new("related", AssociationKind::References, "moments", "momentIds"));
    notes.actions.push(ActionDeclaration::new("create", RootShape::Upsert));
    notes.actions.push(ActionDeclaration::new("update", RootShape::Upsert));
    vec![users_spec(), moments, notes]
}

pub fn mentions(db: &Db, needle: &str) -> Vec<String> {
    let inner = db.shared.store.lock();
    let tables: Vec<String> = inner
        .connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let mut found = Vec::new();
    for table in tables.into_iter().filter(|table| table != "tombstone") {
        let mut statement = inner.connection.prepare(&format!("SELECT * FROM \"{table}\"")).unwrap();
        let columns = statement.column_count();
        let mut rows = statement.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            for column in 0..columns {
                let bytes: Vec<u8> = match row.get_ref(column).unwrap() {
                    rusqlite::types::ValueRef::Text(bytes) | rusqlite::types::ValueRef::Blob(bytes) => bytes.to_vec(),
                    _ => continue,
                };
                if String::from_utf8_lossy(&bytes).contains(needle) {
                    found.push(table.clone());
                }
            }
        }
    }
    found
}

pub fn media_specs() -> Vec<ModelSpec> {
    let mut moments = moments_spec();
    moments.fields.push(FieldSpec::file("media").optional());
    let mut vibes = ModelSpec::new(
        "vibes",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("momentId", ScalarKind::Id).indexed(), FieldSpec::file("media").optional()],
    );
    vibes.associations.push(AssociationSpec::new("moment", AssociationKind::BelongsTo, "moments", "momentId"));
    let mut owned = AssociationSpec::new("vibes", AssociationKind::HasMany, "vibes", "momentId");
    owned.dependent = Some(Dependent::Destroy);
    moments.associations.push(owned);
    vec![users_spec(), moments, vibes]
}

pub fn open_rig(path: &std::path::Path) -> Rig {
    rig_on(Db::open(path, media_specs(), None).unwrap(), MockTransport::new(), SyncConfig::default())
}
