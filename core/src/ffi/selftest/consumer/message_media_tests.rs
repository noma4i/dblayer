//! message.dbl of the app against scenarios.md P-APP-14: the server never empties the media URL and never lowers the transcode progress, on the real feed path.
use serde_json::{json, Value};

use super::stand::{message, Stand};
use crate::ffi::wire::Cell;
use crate::ffi::ReadFetch;

fn media_event(url: &str, progress: i64) -> Value {
    let mut row = message("m1", "c1", "clip");
    row["media"] = json!({"blobId": null, "blurHash": null, "coverUrl": null, "createdAt": "2026-11-08T00:00:00Z", "duration": null, "fileUrl": url, "gifUrl": null, "height": null, "kind": "VIDEO", "thumbUrl": null, "transcoded": false, "transcodeStatus": "PROGRESSING", "transcodeProgress": progress, "transcodeError": null, "updatedAt": "2026-11-08T00:00:00Z", "width": null});
    json!({"area": "MESSAGE", "kind": "UPDATED", "entityId": "m1", "parentId": "c1", "message": row})
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_message_media_keeps_url_and_progress() {
    let stand = Stand::open().await;
    let (_events, token) = stand.events("Message").await;
    let rows = stand.entity("Message", "m1", ReadFetch::Never);
    for (url, progress) in [("", 40), ("https://cdn/a", 80), ("", 30)] {
        stand.push(&token, media_event(url, progress));
        stand.settle().await;
    }
    let row = rows.0.lock().unwrap()[0].clone();
    let spec = stand.database.db.spec("Message").unwrap();
    let members: Vec<&str> = spec.field("media").unwrap().fields.iter().map(|member| member.name.as_str()).collect();
    let Cell::Seq { items } = stand.cell("Message", &row, "media") else { panic!("media is no object") };
    let member = |name: &str| items[members.iter().position(|member| *member == name).unwrap()].clone();
    assert_eq!((member("fileUrl"), member("transcodeProgress")), (Cell::Text { value: "https://cdn/a".into() }, Cell::Real { value: 80.0 }));
}
