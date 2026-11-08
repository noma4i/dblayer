use std::sync::Arc;

use crate::ffi::{Config, Database};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;

async fn notes() -> Arc<Database> {
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    Database::open(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts::default()).await.unwrap()
}

/// out_local_noop_snapshot (P-OUT-37, D-190): a no-op write answers with the row and the revision of one snapshot, and its revision does not grow.
#[tokio::test(flavor = "multi_thread")]
async fn a_noop_write_answers_with_one_snapshot_and_no_new_revision() {
    let database = notes().await;
    let epoch = database.session_epoch();
    let made = database.create(epoch, "Note".into(), r#"{"text":"start"}"#.into(), "{}".into()).await.unwrap();
    let id = made.optimistic_value.unwrap().id;

    let plain = database.update(epoch, "Note".into(), id.clone(), r#"{"text":"start"}"#.into(), "{}".into()).await.unwrap();

    assert_eq!(plain.revision, made.revision, "a no-op makes no new revision");
    assert_eq!(plain.optimistic_value.as_ref().map(|row| row.revision), Some(made.revision));
}

/// out_local_noop_snapshot (P-OUT-37): a write that commits beside the no-op decides which snapshot the no-op shows, and the pair never mixes the row of one with the revision of the other.
#[tokio::test(flavor = "multi_thread")]
async fn a_noop_beside_a_concurrent_write_never_mixes_the_row_and_the_revision() {
    let database = notes().await;
    let epoch = database.session_epoch();
    let made = database.create(epoch, "Note".into(), r#"{"text":"start"}"#.into(), "{}".into()).await.unwrap();
    let id = made.optimistic_value.unwrap().id;

    let (noop, changed) = tokio::join!(
        database.update(epoch, "Note".into(), id.clone(), r#"{"text":"start"}"#.into(), "{}".into()),
        database.update(epoch, "Note".into(), id.clone(), r#"{"text":"other"}"#.into(), "{}".into()),
    );
    let (noop, changed) = (noop.unwrap(), changed.unwrap());

    let shown = format!("{:?}", noop.optimistic_value.as_ref().unwrap().cells);
    let expected = if shown.contains("other") { changed.revision } else { made.revision };
    assert_eq!(noop.revision, expected, "the row {shown} belongs to the revision of its snapshot");
    assert_eq!(noop.optimistic_value.as_ref().map(|row| row.revision), Some(noop.revision));
}
