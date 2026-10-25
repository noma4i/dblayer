use super::{joined, DbError};

#[tokio::test]
async fn a_panicking_read_task_reaches_the_reader_as_an_error_not_as_the_end_of_the_stream() {
    let task = tokio::spawn(async { if true { panic!("read task failed") } None::<u8> });
    assert!(matches!(joined(task).await, Err(DbError::Invalid { .. })));
    let finished = tokio::spawn(async { Some(7u8) });
    assert!(matches!(joined(finished).await, Ok(Some(7))));
}
