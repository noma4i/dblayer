use super::*;

fn keys(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("k{index}")).collect()
}

#[test]
fn chunks_split_stale_keys_by_chunk_size() {
    let result = ResumeDrain::default().chunks(&keys(10)).unwrap();

    assert_eq!(result.iter().map(Vec::len).collect::<Vec<_>>(), vec![4, 4, 2]);
    assert!(ResumeDrain::default().chunks(&[]).unwrap().is_empty());
}

#[test]
fn zero_chunk_size_fails() {
    assert_eq!(
        ResumeDrain::new(std::time::Duration::from_secs(60), 0).chunks(&keys(1)),
        Err(ResumeDrainError::InvalidChunkSize(0))
    );
}
