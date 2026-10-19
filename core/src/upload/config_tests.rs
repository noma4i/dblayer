use super::*;

#[test]
fn independent_clients_randomize_upload_retry_seeds() {
    let pairs = std::array::from_fn::<_, 4, _>(|_| (UploadConfig::default().backoff.seed, UploadConfig::default().backoff.seed));
    assert!(pairs.iter().any(|(first, second)| first != second), "independent upload seeds matched: {pairs:?}");
}
