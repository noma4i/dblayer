use std::time::Duration;

use serde_json::{json, Value};

use crate::db::Db;
use crate::schema::MergeRule;

use super::fixtures::*;
use super::{outranks, CloudChange, Hlc};

struct Conflict {
    name: &'static str,
    a: Value,
    b: Value,
    a_at: u64,
    b_at: u64,
    expect: Value,
}

#[tokio::test]
async fn inv_cloud_03_a_conflict_follows_the_field_rule_and_other_fields_both_survive() {
    let cases = [
        Conflict { name: "last writer wins", a: json!({"title": "a"}), b: json!({"title": "b"}), a_at: 2_000, b_at: 3_000, expect: json!({"title": "b"}) },
        Conflict { name: "last writer wins, other order", a: json!({"title": "a"}), b: json!({"title": "b"}), a_at: 4_000, b_at: 3_000, expect: json!({"title": "a"}) },
        Conflict { name: "monotonic keeps the higher value", a: json!({"rank": 10}), b: json!({"rank": 5}), a_at: 2_000, b_at: 3_000, expect: json!({"rank": 10}) },
        Conflict { name: "different fields both survive", a: json!({"title": "a"}), b: json!({"body": "b"}), a_at: 2_000, b_at: 3_000, expect: json!({"title": "a", "body": "b"}) },
    ];
    for case in cases {
        let fake = FakeCloud::new();
        let (a, b) = (device(&fake, 1_000), device(&fake, 1_000));
        let id = note(&a, json!({"title": "start", "rank": 1}));
        a.cloud.sync_now().await.unwrap();
        b.cloud.sync_now().await.unwrap();
        a.clock.advance(Duration::from_millis(case.a_at - 1_000));
        b.clock.advance(Duration::from_millis(case.b_at - 1_000));
        edit(&a, &id, case.a.clone());
        edit(&b, &id, case.b.clone());
        for device in [&a, &b, &a] {
            device.cloud.sync_now().await.unwrap();
        }
        for (name, want) in case.expect.as_object().unwrap() {
            assert_eq!(&field_of(&a, &id, name), want, "{}: device A, {name}", case.name);
            assert_eq!(&field_of(&b, &id, name), want, "{}: device B, {name}", case.name);
        }
    }
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

fn pick(rng: &mut Lcg, values: &[Value]) -> Value {
    values[rng.next() as usize % values.len()].clone()
}

#[test]
fn inv_cloud_04_the_winner_is_a_total_order_for_every_rule() {
    let rules = [
        None,
        Some(MergeRule::Present),
        Some(MergeRule::NonEmpty),
        Some(MergeRule::Positive),
        Some(MergeRule::Monotonic),
        Some(MergeRule::Ladder { tiers: vec![vec!["low".into()], vec!["mid".into()], vec!["high".into()]] }),
    ];
    let values = [json!(null), json!(0), json!(-2), json!(7), json!(""), json!("low"), json!("mid"), json!("high"), json!("x")];
    let mut rng = Lcg(7);
    for rule in &rules {
        for _ in 0..200 {
            let stamps: Vec<Hlc> = (0..3).map(|at| Hlc::new((rng.next() % 4) as i64, (rng.next() % 3) as u32, ["r1", "r2", "r3"][at])).collect();
            let candidates: Vec<(Value, Hlc)> = stamps.into_iter().map(|stamp| (pick(&mut rng, &values), stamp)).collect();
            let wins = |left: usize, right: usize| outranks(rule.as_ref(), (&candidates[left].0, &candidates[left].1), (&candidates[right].0, &candidates[right].1));
            for left in 0..3 {
                for right in 0..3 {
                    if left != right {
                        assert_ne!(wins(left, right), wins(right, left), "rule {rule:?}: exactly one of {:?} and {:?} wins", candidates[left], candidates[right]);
                    }
                }
            }
            for (x, y, z) in [(0, 1, 2), (1, 2, 0), (2, 0, 1), (0, 2, 1), (1, 0, 2), (2, 1, 0)] {
                if wins(x, y) && wins(y, z) {
                    assert!(wins(x, z), "rule {rule:?}: transitivity over {candidates:?}");
                }
            }
        }
    }
}

fn change(row: &str, field: &str, value: Value, ms: i64, replica: &str) -> CloudChange {
    CloudChange { model: "notes".into(), row_id: row.into(), field: field.into(), clock_ms: ms, clock_count: 0, replica: replica.into(), value }
}

fn issued(at: usize, change: CloudChange) -> CloudChange {
    CloudChange { clock_count: at as u32, ..change }
}

#[test]
fn inv_cloud_04_any_delivery_order_and_repeats_give_the_same_rows() {
    let mut rng = Lcg(11);
    for seed in 0..30u64 {
        let mut rng_seed = Lcg(seed + 100);
        let values = [json!(null), json!("a"), json!("b"), json!(3), json!(9)];
        let mut changes = Vec::new();
        for at in 0..8 {
            let row = if rng_seed.next() % 3 == 0 { "n2" } else { "n1" };
            let field = ["title", "body", "rank"][rng_seed.next() as usize % 3];
            let value = match field {
                "rank" => json!(rng_seed.next() % 10),
                _ => pick(&mut rng_seed, &values[..3]),
            };
            changes.push(issued(at, change(row, field, value, (rng_seed.next() % 5) as i64 + 1, ["r1", "r2", "r3"][at % 3])));
        }
        if seed % 5 == 0 {
            changes.push(CloudChange { field: String::new(), value: Value::Null, ..issued(9, change("n2", "", json!(null), 3, "r2")) });
        }
        let mut outcomes = Vec::new();
        for order in 0..6 {
            let mut shuffled = changes.clone();
            for at in (1..shuffled.len()).rev() {
                shuffled.swap(at, rng.next() as usize % (at + 1));
            }
            if order % 2 == 1 {
                shuffled.extend(changes.iter().take(4).cloned());
            }
            let db = Db::open_in_memory(specs(), None).unwrap();
            for part in shuffled.chunks(3) {
                db.apply_cloud_page(db.generation(), &db.cloud_replica(db.generation()).unwrap(), part, &[1]).unwrap();
            }
            let view: Vec<_> = ["n1", "n2"]
                .iter()
                .map(|id| db.find("notes", &crate::identity::RowId::new(id).unwrap()).unwrap().map(|row| serde_json::Value::Object(row.data.clone()).to_string()))
                .collect();
            outcomes.push(view);
        }
        assert!(outcomes.windows(2).all(|pair| pair[0] == pair[1]), "seed {seed}: {outcomes:?}");
    }
}
