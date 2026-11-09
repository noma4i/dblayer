//! C1: universal properties on seeded traces over two rows of one entity on the schema of the app.
use serde_json::{json, Value};

use super::stand::{moment, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::wire::{Cell, WireRow};
use crate::ffi::ReadFetch;
use crate::test_matrix::Matrix;

struct Seed(u64);

impl Seed {
    fn next(&mut self, below: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % below
    }
}

fn text(stand: &Stand, row: &WireRow, field: &str) -> Option<String> { match stand.cell("Moment", row, field) { Cell::Text { value } => Some(value), _ => None } }

#[tokio::test(flavor = "multi_thread")]
async fn universal_properties_on_seeded_traces() {
    let mut matrix = Matrix::new("C1-universal-properties");
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    for seed in 1..=12u64 {
        let mut random = Seed(seed * 7919 + 13);
        let ids = [format!("s{seed}a"), format!("s{seed}b")];
        let mut local: [Option<String>; 2] = [None, None];
        let mut watchers = Vec::new();
        for id in &ids {
            stand.answer("MomentNode", json!({"node": moment(id, 0)}));
            watchers.push(stand.entity("Moment", id, ReadFetch::Force));
            stand.settle().await;
        }
        let mut trace = Vec::new();
        for step in 0..8u64 {
            let which = random.next(2) as usize;
            let id = ids[which].clone();
            let visits = random.next(50) as i64;
            let action = random.next(4);
            trace.push(format!("{action}:{which}:{visits}"));
            match action {
                0 => { stand.answer("MomentNode", json!({"node": moment(&id, visits)})); let _ = bound(stand.database.node(epoch, "Moment".into(), "node".into(), id.clone(), true)).await; }
                1 => {
                    let edges: Vec<Value> = ids.iter().map(|other| json!({"node": moment(other, visits)})).collect();
                    stand.answer("myMoments", json!({"viewer": {"moments": {"edges": edges, "pageInfo": {"hasNextPage": false, "endCursor": null}, "totalCount": 2}}}));
                    let _ = bound(stand.database.refresh(epoch, "Moment".into(), Some("owned".into()), "{}".into())).await;
                }
                2 => {
                    let value = format!("file:///{seed}-{step}");
                    if bound(stand.database.set_local(epoch, "Moment".into(), id.clone(), "localPreviewUrl".into(), json!(value).to_string())).await.is_ok() { local[which] = Some(value); }
                }
                _ => {
                    let mut answer = moment(&id, visits);
                    answer["reacted"] = json!(true);
                    stand.answer("MomentReact", json!({"momentReact": {"moment": answer}}));
                    let _ = bound(stand.database.act(epoch, "Moment".into(), id.clone(), "react".into(), "{}".into(), "{}".into())).await;
                }
            }
            stand.settle().await;
            for (index, id) in ids.iter().enumerate() {
                let fresh = bound(stand.database.read_row(epoch, "Moment".into(), id.clone())).await.ok().flatten();
                let Some(fresh) = fresh else { matrix.check(&format!("seed {seed} step {step}"), "a row exists", &id.to_string(), "present", "absent"); continue };
                matrix.check(&format!("seed {seed} step {step}"), "local field survives every server path", &format!("{id} after {}", trace.join(" ")), &format!("{:?}", local[index]), &format!("{:?}", text(&stand, &fresh, "localPreviewUrl")));
                let shown = watchers[index].0.lock().unwrap().iter().rev().find(|row| row.id == *id).cloned();
                let same = shown.as_ref().is_some_and(|shown| shown.cells == fresh.cells);
                matrix.check(&format!("seed {seed} step {step}"), "a stream value equals a fresh read", &format!("{id} revision {}", fresh.revision), "equal", if same { "equal" } else { "different" });
            }
        }
    }
    matrix.finish();
}

/// No data effect after the end of the epoch: after `session_reset` the watchers of the old epoch receive nothing, and a call of the old epoch is refused.
#[tokio::test(flavor = "multi_thread")]
async fn no_data_effect_after_the_end_of_the_epoch() {
    let mut matrix = Matrix::new("C1-no-effect-after-epoch-end");
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    let mut seed = Seed(99);
    stand.answer("MomentNode", json!({"node": moment("e1", 0)}));
    let watcher = stand.entity("Moment", "e1", ReadFetch::Force);
    stand.settle().await;
    let before = watcher.0.lock().unwrap().len();
    bound(stand.database.session_reset()).await.unwrap();
    stand.settle().await;
    let after_reset = watcher.0.lock().unwrap().len();
    for step in 0..6u64 {
        let visits = seed.next(50) as i64;
        stand.answer("MomentNode", json!({"node": moment("e1", visits)}));
        let refused = bound(stand.database.node(epoch, "Moment".into(), "node".into(), "e1".into(), true)).await;
        matrix.check(&format!("step {step}"), "a call of the ended epoch", "node", "NoSession", if matches!(refused, Err(crate::ffi::DbError::NoSession { .. })) { "NoSession" } else { "answered" });
        stand.settle().await;
        matrix.check(&format!("step {step}"), "values after the end of the epoch", "watcher of the old epoch", &after_reset.to_string(), &watcher.0.lock().unwrap().len().to_string());
    }
    matrix.check("setup", "the watcher saw the row before the reset", "node page", "at least 1", if before >= 1 { "at least 1" } else { "none" });
    matrix.finish();
}
