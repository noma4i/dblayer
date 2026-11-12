use std::sync::{Arc, Mutex};

use super::crash_image_support::{advancing, drained, timed_stand};
use super::rig_tests::bound;
use super::upload_stand::{clip, Detached, BLOB, CREATE_MOMENT};
use crate::ffi::{ReadRequest, ReadValue};
use crate::owners::write::upload::MEBIBYTE;

const INIT: &str = r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#;
const PRESIGN: &str = r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#;
const COMPLETE: &str = r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#;
const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

#[derive(Debug, PartialEq)]
struct Observed {
    steps: Vec<String>,
    puts: Vec<(u64, u64)>,
    executor_starts: usize,
    never_back: bool,
}

async fn upload(length: u64, executor: Option<Arc<Detached>>, cut_part_two: bool) -> Observed {
    let (stand, clock) = timed_stand();
    let stand = match &executor { Some(executor) => stand.detached_through(executor.clone()), None => stand };
    stand.script.data("InitMultipart", INIT);
    for _ in 0..40 { stand.script.data("PresignParts", PRESIGN); }
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CompleteMultipart", COMPLETE);
    let create = stand.script.hold("CreateMoment", LANDED);
    if cut_part_two { stand.puts.fail_once_at(5 * MEBIBYTE); }
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::File::create(&source).unwrap().set_len(length).unwrap();
    let database = stand.open_with(Some(&directory.path().join("live.sqlite")), &[CREATE_MOMENT], Some(clock.clone())).await;
    let epoch = database.session_epoch();
    let receipt = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    let frames = Arc::new(Mutex::new(Vec::new()));
    let watch = database.subscribe_values(epoch, ReadRequest::Blob { model: "Moment".into(), id, action: "upload_clip".into() });
    let sink = frames.clone();
    let collector = tokio::spawn(async move {
        while let Some(value) = watch.pull_next().await {
            if let ReadValue::Status { progress, .. } = value { sink.lock().unwrap().push(progress); }
        }
    });
    advancing(&clock, create.entered()).await;
    create.release();
    crate::test_wait::settled(&database, &format!("the upload of {length} bytes landed with the executor {}", executor.is_some()), || database.peek_row(epoch, "Moment".into(), "m77".into()).unwrap().is_some()).await;
    drained(&database).await;
    collector.abort();
    let frames = frames.lock().unwrap().clone();
    let mut steps: Vec<String> = stand.script.names();
    steps.dedup();
    let puts = stand.puts.seen.lock().unwrap().iter().map(|put| (put.offset, put.length)).collect();
    Observed {
        steps,
        puts,
        executor_starts: executor.map_or(0, |executor| executor.started().len()),
        never_back: frames.windows(2).all(|pair| pair[0] <= pair[1]),
    }
}

/// The parts of a file do not depend on the executor of the PUT: a file larger than the part size goes in several PUTs in the same order of steps with or without the executor, the cut part two goes again alone, and a file below the threshold goes the same single way.
/// Observable: the calls to the server, the offsets and lengths of the PUTs of the transport, the starts the executor got, the progress frames. Catches: a Sizing that reads the executor and sends one part as large as the file.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_parts_do_not_depend_on_the_put_executor() {
    let cases: [(&str, u64, bool, Vec<&str>, Vec<(u64, u64)>); 2] = [
        ("above the part size, part two cut", 11 * MEBIBYTE, true, vec!["InitMultipart", "PresignParts", "CompleteMultipart", "CreateMoment"], vec![(0, 5 * MEBIBYTE), (5 * MEBIBYTE, 5 * MEBIBYTE), (5 * MEBIBYTE, 5 * MEBIBYTE), (10 * MEBIBYTE, MEBIBYTE)]),
        ("below the threshold", MEBIBYTE, false, vec!["CreateBlob", "CreateMoment"], vec![(0, MEBIBYTE)]),
    ];
    for (name, length, cut, steps, puts) in cases {
        let without = upload(length, None, cut).await;
        let with = upload(length, Some(Arc::new(Detached::default())), cut).await;
        assert_eq!(without, with, "{name}: the same upload with and without the executor");
        assert_eq!((without.executor_starts, without.never_back), (0, true), "{name}");
        assert_eq!(without.puts, puts, "{name}: sizes of the PUTs, part one is not sent again");
        assert_eq!(without.steps.iter().filter(|step| steps.contains(&step.as_str())).cloned().collect::<Vec<_>>(), steps, "{name}: the order of steps");
    }
}
