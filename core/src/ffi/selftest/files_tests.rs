use crate::ffi::{Config, Database, DbError};

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_url() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("files.sqlite");
    let mut local = crate::schema::FieldSpec::object("draft").optional();
    local.fields = vec![crate::schema::FieldSpec::scalar("text", crate::value::ScalarKind::Str).optional()];
    local.merge = Some(crate::schema::MergeRule::Local);
    let mut spec = crate::schema::ModelSpec::new("Files", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::file("photo").nullable(), local]);
    spec.storage = crate::schema::Storage::Local;
    let models = serde_json::to_string(&vec![spec]).unwrap();
    let config = Config { path: Some(path.to_string_lossy().into()) };
    let db = Database::open(config, crate::ffi::specs(models), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = db.session_epoch();
    let id = db.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    assert_eq!(db.url(epoch, "Files".into(), id.clone(), "photo".into()).await.unwrap(), None);
    assert!(matches!(db.url(epoch, "Files".into(), id.clone(), "id".into()).await, Err(DbError::Invalid { .. })));
    for (path, value, expected) in [("photo", "null", "photo"), ("draft", "{\"unknown\":1}", "draft.unknown"), ("draft", "{\"text\":1}", "draft")] {
        let result = db.set_local(epoch, "Files".into(), id.clone(), path.into(), value.into()).await;
        assert!(matches!(result, Err(DbError::Invalid { detail }) if detail.contains(expected)));
    }
    assert!(matches!(db.url(epoch + 1, "Files".into(), id.clone(), "photo".into()).await, Err(DbError::NoSession { .. })));
    let source = directory.path().join("source");
    std::fs::write(&source, vec![7; 300 * 1024]).unwrap();
    db.attach_file(epoch, "Files".into(), id.clone(), "photo".into(), source.to_string_lossy().into(), "large".into(), "image/png".into()).await.unwrap();
    let url = db.url(epoch, "Files".into(), id.clone(), "photo".into()).await.unwrap().unwrap();
    assert_eq!(std::fs::read(url).unwrap(), vec![7; 300 * 1024]);
    db.close_database().await.unwrap();
    assert!(matches!(db.url(epoch, "Files".into(), id, "photo".into()).await, Err(DbError::Closed { .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attach_detach() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("files.sqlite");
    let mut spec = crate::schema::ModelSpec::new("Files", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::file("photo").nullable()]);
    spec.storage = crate::schema::Storage::Local;
    let models = serde_json::to_string(&vec![spec]).unwrap();
    let config = || Config { path: Some(path.to_string_lossy().into()) };
    let db = Database::open(config(), crate::ffi::specs(models.clone()), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = db.session_epoch();
    let first = db.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    let second = db.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    let small = vec![1, 2, 3];
    let large = vec![9; 300 * 1024];
    let source = directory.path().join("source");
    std::fs::write(&source, &small).unwrap();
    db.attach_file(epoch, "Files".into(), first.clone(), "photo".into(), source.to_string_lossy().into(), "small".into(), "image/png".into()).await.unwrap();
    std::fs::write(&source, &large).unwrap();
    db.attach_file(epoch, "Files".into(), second.clone(), "photo".into(), source.to_string_lossy().into(), "large".into(), "image/png".into()).await.unwrap();
    let first_url = db.url(epoch, "Files".into(), first.clone(), "photo".into()).await.unwrap().unwrap();
    assert_eq!(std::fs::read(first_url).unwrap(), small);
    let url = db.url(epoch, "Files".into(), second.clone(), "photo".into()).await.unwrap().unwrap();
    assert_eq!(std::fs::read(url).unwrap(), large);
    std::fs::write(&source, [4]).unwrap();
    db.attach_file(epoch, "Files".into(), first.clone(), "photo".into(), source.to_string_lossy().into(), "replacement".into(), "image/png".into()).await.unwrap();
    assert!(db.detach(epoch, "Files".into(), first.clone(), "photo".into()).await.unwrap());
    db.close_database().await.unwrap();
    drop(db);
    let db = Database::open(config(), crate::ffi::specs(models), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = db.session_epoch();
    assert_eq!(db.url(epoch, "Files".into(), first, "photo".into()).await.unwrap(), None);
    let second_url = db.url(epoch, "Files".into(), second.clone(), "photo".into()).await.unwrap().unwrap();
    assert_eq!(std::fs::read(second_url).unwrap(), large);
    let info = db.file(epoch, "Files".into(), second, "photo".into()).await.unwrap().unwrap();
    assert_eq!(info.name.as_deref(), Some("large"));
    assert_eq!(info.size, 300 * 1024);
    db.close_database().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attach_url_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("files.sqlite");
    let mut spec = crate::schema::ModelSpec::new("Files", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::file("photo").nullable()]);
    spec.storage = crate::schema::Storage::Local;
    let db = Database::open(Config { path: Some(path.to_string_lossy().into()) }, crate::ffi::specs(serde_json::to_string(&vec![spec]).unwrap()), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = db.session_epoch();
    let id = db.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    for size in [0usize, 256 * 1024, 256 * 1024 + 1, 2 * 1024 * 1024] {
        let source = directory.path().join(format!("source-{size}"));
        let bytes = vec![23u8; size];
        std::fs::write(&source, &bytes).unwrap();
        let info = db.attach_file(epoch, "Files".into(), id.clone(), "photo".into(), source.to_string_lossy().into(), "photo.bin".into(), "application/octet-stream".into()).await.unwrap();
        assert_eq!(info.size, size as u64);
        assert_eq!(info.name.as_deref(), Some("photo.bin"));
        assert_eq!(info.mime.as_deref(), Some("application/octet-stream"));
        let url = db.url(epoch, "Files".into(), id.clone(), "photo".into()).await.unwrap().unwrap();
        assert_eq!(std::fs::read(url).unwrap(), bytes);
        assert_eq!(std::fs::read(source).unwrap(), bytes);
    }
    db.close_database().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn out_local_attach_missing_row() {
    let directory = tempfile::tempdir().unwrap();
    let mut spec = crate::schema::ModelSpec::new("Files", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::file("photo").nullable()]);
    spec.storage = crate::schema::Storage::Local;
    let models = serde_json::to_string(&vec![spec]).unwrap();
    let config = Config { path: Some(directory.path().join("files.sqlite").to_string_lossy().into()) };
    let db = Database::open(config, crate::ffi::specs(models), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = db.session_epoch();
    let source = directory.path().join("source");
    std::fs::write(&source, [1]).unwrap();
    let attached = db.attach_file(epoch, "Files".into(), "absent".into(), "photo".into(), source.to_string_lossy().into(), "a".into(), "image/png".into()).await;
    let detached = db.detach(epoch, "Files".into(), "absent".into(), "photo".into()).await;
    assert!((matches!(attached, Err(DbError::NotFound { .. })), matches!(detached, Err(DbError::NotFound { .. }))) == (true, true), "attach and detach of a missing row answer NotFound and copy no file: {attached:?} {detached:?}");
}
