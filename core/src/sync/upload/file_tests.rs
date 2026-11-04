use super::super::rig::*;
use super::upload_tests::{file, input, multipart_replies, rid, single_replies, small, upload_rig};
use crate::upload::UploadState;

fn stored_files(rig: &Rig) -> i64 {
    rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT COUNT(*) FROM file", [], |row| row.get::<_, i64>(0))?)).unwrap()
}

#[test]
fn inv_file_05_maintain_and_sweep_files_keep_the_file_of_an_open_operation_until_it_closes() { crate::test_wait::paused(async {
    let rig = upload_rig(small());
    let source = file(b"abcdef");
    (rig.sync.inner.begin_upload(&rig.sync.inner.db.admit().unwrap(), rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), UploadState::queued())).unwrap();
    let held = stored_files(&rig);
    assert_eq!(held, 1);
    rig.db.sweep_files(&rig.db.admit().unwrap()).unwrap();
    rig.db.maintain(&rig.db.admit().unwrap(), &[]).unwrap();
    assert_eq!(stored_files(&rig), held);
    assert_eq!(rig.db.discard(&rig.db.admit().unwrap(), "moments", &rid("m1"), None).unwrap(), 1);
    rig.db.sweep_files(&rig.db.admit().unwrap()).unwrap();
    assert_eq!(stored_files(&rig), 0);
}) }

#[test]
fn inv_file_08_a_file_is_read_in_ranges_up_to_the_single_threshold_or_the_part_size() { crate::test_wait::paused(async {
    let single = upload_rig(small());
    single_replies(&single);
    let below_threshold = file(b"012345678");
    crate::sync::rig::app(&single.sync, single.sync.upload(&single.sync.inner.db.admit().unwrap(), "moments", &rid("m1"), "upload", input(&below_threshold))).await.unwrap();
    single.sync.wait_uploads().await;
    assert_eq!(single.transport.put_calls().iter().map(|put| put.body.len()).collect::<Vec<_>>(), [9]);

    let multipart = upload_rig(small());
    multipart_replies(&multipart);
    let over_threshold = file(b"0123456789a");
    crate::sync::rig::app(&multipart.sync, multipart.sync.upload(&multipart.sync.inner.db.admit().unwrap(), "moments", &rid("m1"), "upload", input(&over_threshold))).await.unwrap();
    multipart.sync.wait_uploads().await;
    let mut sizes: Vec<usize> = multipart.transport.put_calls().iter().map(|put| put.body.len()).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, [1, 5, 5]);
}) }
