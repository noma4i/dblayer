use super::*;

fn options() -> OpenOptions { OpenOptions { endpoint: None, headers: HashMap::new(), operations: "[]".into(), references: vec![1, 2, 3] } }
async fn open(slot: &OpenDatabase, options: OpenOptions) -> Result<Arc<Database>, DbError> {
    slot.open(Config { path: None, data_version: None, cache_rows: 8 }, "[]".into(), options, None, None, None, None).await
}

#[tokio::test]
async fn opening_reuses_equal_arguments_and_refuses_each_changed_argument() {
    let slot = OpenDatabase::new();
    let (first, second) = tokio::join!(open(&slot, options()), open(&slot, options()));
    let first = first.unwrap();
    assert!(Arc::ptr_eq(&first, &second.unwrap()), "equal concurrent opens created separate databases");
    let mut changes = Vec::new();
    let mut changed = options(); changed.endpoint = Some("https://other.example".into()); changes.push(changed);
    let mut changed = options(); changed.headers.insert("Authorization".into(), "other".into()); changes.push(changed);
    let mut changed = options(); changed.operations = r#"[{"name":"Other","document":"query Other { value }"}]"#.into(); changes.push(changed);
    for index in 0..3 { let mut changed = options(); changed.references[index] += 10; changes.push(changed); }
    for changed in changes { assert!(matches!(open(&slot, changed).await, Err(DbError::Invalid { .. }))); }
    for config in [Config { path: Some("different.sqlite".into()), data_version: None, cache_rows: 8 }, Config { path: None, data_version: Some("other".into()), cache_rows: 8 }, Config { path: None, data_version: None, cache_rows: 16 }] {
        assert!(matches!(slot.open(config, "[]".into(), options(), None, None, None, None).await, Err(DbError::Invalid { .. })));
    }
    assert!(matches!(slot.open(Config { path: None, data_version: None, cache_rows: 8 }, "invalid".into(), options(), None, None, None, None).await, Err(DbError::Invalid { .. })));

    let slot = OpenDatabase::new();
    let mut first_options = options(); first_options.operations = r#"[{"name":"Read","document":"query Read { value }"}]"#.into();
    let first = open(&slot, first_options).await.unwrap();
    let mut reordered = options(); reordered.operations = r#"[{"document":"query Read { value }","name":"Read"}]"#.into();
    assert!(Arc::ptr_eq(&first, &open(&slot, reordered).await.unwrap()));
    let slot = OpenDatabase::new();
    assert!(slot.open(Config { path: None, data_version: None, cache_rows: 8 }, "invalid".into(), options(), None, None, None, None).await.is_err());
    assert!(open(&slot, options()).await.is_ok());
}
