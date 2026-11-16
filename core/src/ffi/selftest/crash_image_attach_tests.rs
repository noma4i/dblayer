//! A process killed after any commit of an `attach` that replaces the file of a file field leaves a file that reopens showing one whole file and no leftover. Requirement -> observed value -> assert.

use super::crash_image_classes_tests::flips;
use super::crash_image_support::{checkpoint_live, copy_tree, files_of, over_images, Capture};
use crate::ffi::{Config, Database};

fn models() -> String {
    let mut spec = crate::schema::ModelSpec::new("Files", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::file("photo").nullable()]);
    spec.storage = crate::schema::Storage::Local;
    serde_json::to_string(&vec![spec]).unwrap()
}

async fn open(path: &std::path::Path) -> std::sync::Arc<Database> {
    Database::open(Config { path: Some(path.to_string_lossy().into()) }, crate::ffi::specs(models()), crate::ffi::OpenPorts::default()).await.unwrap()
}

async fn attach(database: &Database, epoch: u64, id: &str, source: &std::path::Path, name: &str) {
    database.attach_file(epoch, "Files".into(), id.into(), "photo".into(), source.to_string_lossy().into(), name.into(), "image/png".into()).await.unwrap();
}

struct Cut {
    bytes: Vec<u8>,
    size: u64,
    entries: usize,
}

/// A second large file attached over the first, killed at any commit: the row shows the first or the second file whole, its descriptor matches the bytes, and the files directory holds exactly the shown file, whether the copy or the removal of the old one was cut.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_an_attach_reopens_with_one_whole_file_and_no_leftover() {
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("files.sqlite");
    let (first, second) = (vec![1u8; 300 * 1024], vec![2u8; 301 * 1024]);
    let source = directory.path().join("source");
    let database = open(&live).await;
    let epoch = database.session_epoch();
    let id = database.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    std::fs::write(&source, &first).unwrap();
    attach(&database, epoch, &id, &source, "first").await;
    checkpoint_live(&database, &live).await;
    let base = Capture::begin(&live);
    let kept = directory.path().join("kept.files");
    copy_tree(&files_of(&live), &kept);
    std::fs::write(&source, &second).unwrap();
    attach(&database, epoch, &id, &source, "second").await;
    let (capture, files) = base.end(&live);
    copy_tree(&files, &kept);

    let cuts = over_images(&capture, &kept, |_, image| {
        let id = id.clone();
        async move {
            let reopened = open(&image).await;
            let epoch = reopened.session_epoch();
            let url = reopened.url(epoch, "Files".into(), id.clone(), "photo".into()).await.unwrap().expect("the row shows a file");
            let bytes = std::fs::read(url).unwrap();
            let size = reopened.file(epoch, "Files".into(), id, "photo".into()).await.unwrap().expect("the row has a descriptor").size;
            reopened.close_database().await.unwrap();
            let entries = std::fs::read_dir(files_of(&image)).unwrap().filter(|entry| entry.as_ref().unwrap().file_type().unwrap().is_file()).count();
            Cut { bytes, size, entries }
        }
    }).await;
    for (index, cut) in cuts.iter().enumerate() {
        assert!(cut.bytes == first || cut.bytes == second, "cut {index}: the shown file is neither whole file: {} bytes", cut.bytes.len());
        assert_eq!(cut.size, cut.bytes.len() as u64, "cut {index}: the descriptor matches the bytes");
        assert_eq!(cut.entries, 1, "cut {index}: the files directory holds the shown file only");
    }
    let second_shown: Vec<bool> = cuts.iter().map(|cut| cut.bytes == second).collect();
    assert!(!second_shown[0] && *second_shown.last().unwrap() && flips(&second_shown) == 1, "the first file is replaced by the second once: {second_shown:?}");
    database.close_database().await.unwrap();
}
