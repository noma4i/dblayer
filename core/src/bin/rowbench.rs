use std::time::Instant;

use dblayer::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use dblayer::db::{Db, DbConfig};
use dblayer::identity::{ModelKey, RowId};
use dblayer::query::{Query, Where};
use dblayer::schema::{FieldSpec, ModelSpec};
use dblayer::value::ScalarKind;
use serde_json::{json, Map, Value};

const USAGE: &str = "usage: rowbench [--rows N] [--repeats N]";
const OPERATIONS: [&str; 5] = ["upsert", "read_ids", "read_query", "update_one_field", "filter_plain"];

fn spec() -> ModelSpec {
    ModelSpec::new(
        "bench",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("ownerId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("title", ScalarKind::Str),
            FieldSpec::scalar("count", ScalarKind::Int),
            FieldSpec::scalar("ratio", ScalarKind::Num),
            FieldSpec::scalar("flag", ScalarKind::Bool),
            FieldSpec::scalar("createdAt", ScalarKind::Date),
            FieldSpec::list("labels", ScalarKind::Str),
            FieldSpec::object("meta").nullable(),
        ],
    )
}

fn row(index: usize, count: i64) -> Map<String, Value> {
    json!({
        "id": format!("r{index}"),
        "ownerId": format!("o{}", index % 20),
        "title": format!("title number {index}"),
        "count": count,
        "ratio": index as f64 / 7.0,
        "flag": index % 2 == 0,
        "createdAt": "2026-10-14T00:00:00.000Z",
        "labels": ["a", "b", "c"],
        "meta": {"title": "t", "tags": [1, 2, 3], "inner": {"x": index}},
    })
    .as_object()
    .cloned()
    .expect("row is an object")
}

fn land(db: &Db, rows: usize, count: i64) {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops = (0..rows)
        .map(|index| {
            let row = row(index, count);
            EntityOp::Upsert { model: ModelKey::new("bench"), id: RowId::new(&format!("r{index}")).expect("id"), row }
        })
        .collect();
    db.land(envelope).expect("land");
}

fn run(rows: usize) -> [f64; 5] {
    let directory = tempfile::tempdir().expect("directory");
    let db = Db::open_with(Some(&directory.path().join("bench.db")), vec![spec()], None, DbConfig { cache_rows: 1 }).expect("open");
    let ids: Vec<Value> = (0..rows).map(|index| json!(format!("r{index}"))).collect();
    let mut seconds = [0.0; 5];
    let mut time = |slot: usize, work: &dyn Fn()| {
        let started = Instant::now();
        work();
        seconds[slot] = started.elapsed().as_secs_f64() * 1000.0;
    };
    time(0, &|| land(&db, rows, 1));
    time(1, &|| assert_eq!(db.query(&Query::new("bench").filtered(Where::in_list("id", ids.clone()))).expect("read").len(), rows));
    time(2, &|| assert_eq!(db.query(&Query::new("bench")).expect("query").len(), rows));
    time(3, &|| land(&db, rows, 2));
    time(4, &|| assert_eq!(db.query(&Query::new("bench").filtered(Where::equal("count", json!(2)))).expect("filter").len(), rows));
    seconds
}

fn main() {
    let (mut rows, mut repeats) = (1000usize, 5usize);
    let mut input = std::env::args().skip(1);
    while let Some(flag) = input.next() {
        let value = input.next().and_then(|text| text.parse::<usize>().ok());
        match (flag.as_str(), value) {
            ("--rows", Some(value)) => rows = value,
            ("--repeats", Some(value)) => repeats = value,
            _ => {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
        }
    }
    let samples: Vec<[f64; 5]> = (0..repeats).map(|_| run(rows)).collect();
    for (slot, name) in OPERATIONS.iter().enumerate() {
        let mut column: Vec<f64> = samples.iter().map(|sample| sample[slot]).collect();
        column.sort_by(f64::total_cmp);
        println!("{name} rows={rows} repeats={repeats} min={:.2}ms median={:.2}ms max={:.2}ms", column[0], column[column.len() / 2], column[column.len() - 1]);
    }
}
