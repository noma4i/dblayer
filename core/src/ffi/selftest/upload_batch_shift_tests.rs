use crate::ffi::upload_wire::WireUploadItem;

pub(super) const OPERATIONS: [(&str, &str); 3] = [
    ("CreateBlob", "mutation CreateBlob { createBlob { url headers signedId blobId } }"),
    ("CreateAssets", "mutation CreateAssets { createAssets { assets { id clientKey size position } } }"),
    ("CreateAsset", "mutation CreateAsset { createAsset { asset { id clientKey size position } } }"),
];
pub(super) const ASSETS: &str = r#"{"data":{"createAssets":{"assets":[{"id":"a2","clientKey":"key-2","size":2,"position":1},{"id":"a1","clientKey":"key-1","size":2,"position":0}]}}}"#;

pub(super) fn blob(index: usize) -> String { format!(r#"{{"data":{{"blobCreate":{{"url":"https://storage/file","headers":{{}},"signedId":"signed-{index}","blobId":"b{index}"}}}}}}"#) }

pub(super) fn items(directory: &std::path::Path) -> Vec<WireUploadItem> {
    (0..3).map(|index| {
        let path = directory.join(format!("{index}.jpg"));
        std::fs::write(&path, format!("image-{index}")).unwrap();
        WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":3,"position":{index}}}"#), source: crate::ffi::upload_wire::WireSource::File { path: path.to_str().unwrap().into(), name: None, mime: None, prepare: None }, variables_json: "{}".into() }
    }).collect()
}
