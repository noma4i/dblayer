use std::sync::Arc;

use async_trait::async_trait;

use super::rig_tests::bound;
use super::upload_stand::{Stand, CREATE_MOMENT};
use crate::ffi::upload_wire::{WirePrepare, WireSource, WireUploadRequest};
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::{DbError, PrepareJob, PrepareOutcome, PrepareProgress, Preparer, ProbeOutcome};
use crate::prepare::{default_video_limits, CropFractions, ImageFormat, PhotoEdit, PixelSize, VideoLimits};

struct Refusing;

#[async_trait]
impl Preparer for Refusing {
    async fn probe(&self, _: String) -> ProbeOutcome { ProbeOutcome::Failed { message: "unused".into() } }
    async fn run(&self, _: PrepareJob, _: Arc<PrepareProgress>) -> PrepareOutcome { PrepareOutcome::Failed { message: "unused".into() } }
    fn cancel(&self, _: u64) {}
}

fn video(change: impl FnOnce(&mut VideoLimits)) -> WirePrepare {
    let mut limits = default_video_limits();
    change(&mut limits);
    WirePrepare::Video { limits: Some(limits), mirror: false }
}

fn image(change: impl FnOnce(&mut PhotoEdit)) -> WirePrepare {
    let mut edit = PhotoEdit { max_size: None, enlarge: false, crop: None, output_size: None, mirror: false, rotation_degrees: 0 };
    change(&mut edit);
    WirePrepare::Image { edit, quality: None, format: ImageFormat::Jpeg }
}

fn send(epoch: u64, path: &std::path::Path, prepare: WirePrepare) -> CommandRequest {
    let source = WireSource::File { path: path.to_str().unwrap().into(), name: None, mime: None, prepare: Some(prepare) };
    CommandRequest { epoch, model: "Moment".into(), values_json: r#"{"text":"clip"}"#.into(), args_json: "{}".into(), upload: Some(CommandUpload { action: "upload_clip".into(), request: WireUploadRequest { source, variables_json: "{}".into() } }) }
}

fn transfer_files(database: &std::path::Path) -> usize {
    std::fs::read_dir(format!("{}.files/transfers", database.display())).map_or(0, |found| found.count())
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_invalid_settings() {
    let directory = tempfile::tempdir().unwrap();
    let (path, source) = (directory.path().join("db.sqlite"), directory.path().join("clip"));
    std::fs::write(&source, "clip").unwrap();
    let stand = Stand::new().with_preparer(Arc::new(Refusing));
    let database = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    let rows = |database: &crate::ffi::Database| database.db.lock().connection.query_row("SELECT (SELECT count(*) FROM sync_upload) + (SELECT count(*) FROM sync_upload_prepare) + (SELECT count(*) FROM sync_upload_prepare_step)", [], |found| found.get::<_, i64>(0)).unwrap();
    let invalid: Vec<(&str, WirePrepare)> = vec![
        ("long side below 2", video(|limits| limits.max_long_side = 0)),
        ("no corridors", video(|limits| limits.corridors.clear())),
        ("min above max bitrate", video(|limits| limits.corridors[0].min_bps = limits.corridors[0].max_bps + 1)),
        ("frame rate of 0", video(|limits| limits.min_frame_rate = 0)),
        ("max size side 0", image(|edit| edit.max_size = Some(PixelSize { width: 0, height: 10 }))),
        ("output size side 0", image(|edit| edit.output_size = Some(PixelSize { width: 10, height: 0 }))),
        ("crop outside 0..1", image(|edit| edit.crop = Some(CropFractions { x: 1.5, y: 0.0, width: 0.5, height: 0.5 }))),
        ("crop without pixels", image(|edit| edit.crop = Some(CropFractions { x: 0.0, y: 0.0, width: 0.0, height: 0.5 }))),
        ("rotation 45", image(|edit| edit.rotation_degrees = 45)),
        ("rotation 360", image(|edit| edit.rotation_degrees = 360)),
        ("quality 1.5", WirePrepare::Image { edit: PhotoEdit { max_size: None, enlarge: false, crop: None, output_size: None, mirror: false, rotation_degrees: 0 }, quality: Some(1.5), format: ImageFormat::Jpeg }),
    ];
    for (name, prepare) in invalid {
        let refused = bound(database.send(send(epoch, &source, prepare))).await.map(|_| ());
        assert!(matches!(refused, Err(DbError::Invalid { .. })), "{name}: {refused:?}");
    }
    assert_eq!((rows(&database), transfer_files(&path)), (0, 0));
    database.close_now();

    let without = Stand::new();
    let database = without.open(Some(&directory.path().join("other.sqlite")), &[CREATE_MOMENT]).await;
    let refused = bound(database.send(send(database.session_epoch(), &source, WirePrepare::Video { limits: None, mirror: false }))).await.map(|_| ());
    assert!(matches!(refused, Err(DbError::Invalid { .. })), "no preparer: {refused:?}");
    database.close_now();
}
