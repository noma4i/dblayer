use std::sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}};
use std::collections::BTreeMap;

use serde_json::json;

use super::stand::{moment, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::Cell;

struct SqlCount {
    keys: [AtomicU64; 2048],
    counts: [AtomicUsize; 2048],
}

impl SqlCount {
    fn new() -> Self { Self { keys: std::array::from_fn(|_| AtomicU64::new(0)), counts: std::array::from_fn(|_| AtomicUsize::new(0)) } }
    fn snapshot(&self) -> BTreeMap<u64, usize> {
        self.keys.iter().zip(&self.counts).filter_map(|(key, count)| {
            let key = key.load(Ordering::Relaxed);
            (key != 0).then(|| (key, count.load(Ordering::Relaxed)))
        }).collect()
    }
}

unsafe extern "C" fn count_selects(_: u32, context: *mut std::ffi::c_void, _: *mut std::ffi::c_void, sql: *mut std::ffi::c_void) -> i32 {
    let text = unsafe { std::ffi::CStr::from_ptr(sql.cast()) }.to_bytes();
    if !text.starts_with(b"SELECT") { return 0; }
    let hash = text.iter().fold(0xcbf29ce484222325u64, |hash, byte| (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)).max(1);
    let capture = unsafe { &*context.cast::<SqlCount>() };
    for offset in 0..capture.keys.len() {
        let index = (hash as usize).wrapping_add(offset) % capture.keys.len();
        let slot = &capture.keys[index];
        if slot.load(Ordering::Relaxed) == hash || slot.compare_exchange(0, hash, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            capture.counts[index].fetch_add(1, Ordering::Relaxed);
            break;
        }
    }
    0
}

async fn reference_batch(size: usize) -> BTreeMap<u64, usize> {
    let stand = Stand::open().await;
    let start = stand.request_mark();
    let epoch = stand.database.session_epoch();
    let user = |id: String| json!({"__typename":"User", "id":id, "fullName":"A", "username":"a", "online":false, "createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"});
    let visitors: Vec<_> = (0..2).map(|i| json!({"node":user(format!("u{i}"))})).collect();
    let mut node = moment("root", 0);
    node["visitors"] = json!({"edges":visitors});
    stand.answer("MomentNode", json!({"node":node}));
    let first = bound(stand.database.node(epoch, "Moment".into(), "node".into(), "root".into(), true)).await.unwrap().unwrap();
    stand.settle().await;
    let ids = |prefix: &str| Cell::Seq { items: (0..size).map(|i| Cell::Text { value: format!("{prefix}{i}") }).collect() };
    let visitor_ids = Cell::Seq { items: (0..2).map(|i| Cell::Text { value: format!("u{i}") }).collect() };
    assert_eq!(stand.cell("Moment", &first, "visitorIds"), visitor_ids);
    let similar: Vec<_> = (0..size).map(|i| json!({"unseen":false, "moment":moment(&format!("m{i}"), 0)})).collect();
    let mut root = moment("root", 0);
    root["similarMoments"] = json!(similar);
    stand.answer("meCompass", json!({"viewer":{"compassMoments":{"edges":[{"node":root}], "pageInfo":{"hasNextPage":false,"endCursor":null}}}}));
    stand.settle().await;
    let received = stand.request_mark();
    stand.hold(&["meCompass"]);
    let database = stand.database.clone();
    let refresh = tokio::spawn(async move { database.refresh(epoch, "Moment".into(), Some("compass".into()), "{}".into()).await });
    stand.receive_after(received, "meCompass", None, 1).await;
    let selects = Arc::new(SqlCount::new());
    let connection = stand.database.db.lock();
    let result = unsafe { rusqlite::ffi::sqlite3_trace_v2(connection.connection.handle(), rusqlite::ffi::SQLITE_TRACE_STMT, Some(count_selects), Arc::as_ptr(&selects).cast_mut().cast()) };
    assert_eq!(result, rusqlite::ffi::SQLITE_OK);
    drop(connection);
    stand.release(&["meCompass"]);
    bound(refresh).await.unwrap().unwrap();
    stand.settle().await;
    stand.database.db.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::empty(), None);
    let reads = selects.snapshot();
    let row = bound(stand.database.read_row(epoch, "Moment".into(), "root".into())).await.unwrap().unwrap();
    assert_eq!(stand.cell("Moment", &row, "visitorIds"), visitor_ids);
    assert_eq!(stand.cell("Moment", &row, "similarIds"), ids("m"));
    for i in 0..2 { assert!(bound(stand.database.read_row(epoch, "User".into(), format!("u{i}"))).await.unwrap().is_some()); }
    for i in 0..size { assert!(bound(stand.database.read_row(epoch, "Moment".into(), format!("m{i}"))).await.unwrap().is_some()); }
    assert_eq!((stand.requests_after(start, "MomentNode", None), stand.requests_after(start, "meCompass", None)), (1, 1));
    stand.database.close_now();
    reads
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_references_edges_batch() {
    let small = reference_batch(2).await;
    let large = reference_batch(12).await;
    let differences: Vec<_> = small.keys().chain(large.keys()).copied().collect::<std::collections::BTreeSet<_>>().into_iter().filter_map(|key| {
        let pair = (small.get(&key).copied().unwrap_or(0), large.get(&key).copied().unwrap_or(0));
        (pair.0 != pair.1).then_some((format!("{key:016x}"), pair))
    }).collect();
    assert_eq!(large.values().sum::<usize>(), small.values().sum::<usize>(), "SQL SELECT counts by hash: {differences:?}");
}
