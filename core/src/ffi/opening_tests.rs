use super::*;

fn config() -> Config { Config { path: None, data_version: None, cache_rows: 8 } }
fn options() -> OpenOptions { OpenOptions { operations: "[]".into() } }
async fn open(slot: &OpenDatabase) -> Result<Arc<Database>, DbError> {
    slot.open(config(), "[]".into(), options(), None, None, None, None).await
}

#[tokio::test]
async fn a_second_open_is_refused_even_with_equal_arguments_until_the_database_is_closed() {
    let slot = OpenDatabase::new();
    let first = open(&slot).await.unwrap();
    for _ in 0..2 {
        match open(&slot).await {
            Err(DbError::Invalid { detail }) => assert_eq!(detail, "database is already open; open it once at app start"),
            other => panic!("second open gave {:?}", other.map(|_| ())),
        }
    }
    first.shutdown();
    let second = open(&slot).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(open(&slot).await.is_err());
}

#[tokio::test]
async fn an_open_that_fails_leaves_the_slot_free() {
    let slot = OpenDatabase::new();
    assert!(slot.open(config(), "invalid".into(), options(), None, None, None, None).await.is_err());
    assert!(open(&slot).await.is_ok());
}
