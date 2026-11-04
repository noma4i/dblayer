use std::collections::BTreeMap;
use std::ffi::{c_int, c_uint, c_void, CStr};
use std::sync::Mutex;
use std::time::Instant;

use dblayer::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use dblayer::db::{Db, DbConfig};
use dblayer::identity::{ModelKey, RowId};
use dblayer::query::{Query, Where};
use dblayer::schema::{FieldSpec, ModelSpec};
use dblayer::value::ScalarKind;
use rusqlite::ffi;
use serde_json::{json, Map, Value};

#[derive(Clone, Copy, PartialEq)]
enum Shape {
    Scalars,
    List,
    Object,
    All,
}

impl Shape {
    fn parse(text: &str) -> Option<Self> {
        match text {
            "scalars" => Some(Self::Scalars),
            "list" => Some(Self::List),
            "object" => Some(Self::Object),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    fn has_list(self) -> bool {
        matches!(self, Self::List | Self::All)
    }

    fn has_object(self) -> bool {
        matches!(self, Self::Object | Self::All)
    }
}

static STATEMENTS: Mutex<BTreeMap<String, u64>> = Mutex::new(BTreeMap::new());

fn bucket(sql: &str) -> String {
    let words: Vec<&str> = sql.split_whitespace().collect();
    let after = |marker: &str| words.iter().position(|word| word.eq_ignore_ascii_case(marker)).and_then(|at| words.get(at + 1)).map(|word| word.trim_matches(|c| c == '(' || c == ')' || c == ',').to_string());
    let verb = words.first().map(|word| word.to_ascii_uppercase()).unwrap_or_default();
    let table = match verb.as_str() {
        "INSERT" => after("INTO"),
        "UPDATE" => after("UPDATE"),
        _ => after("FROM"),
    };
    format!("{verb} {}", table.unwrap_or_default())
}

unsafe extern "C" fn on_statement(_mask: c_uint, _context: *mut c_void, _statement: *mut c_void, sql: *mut c_void) -> c_int {
    if !sql.is_null() {
        let text = unsafe { CStr::from_ptr(sql as *const _) }.to_string_lossy().into_owned();
        *STATEMENTS.lock().expect("counts").entry(bucket(&text)).or_insert(0) += 1;
    }
    0
}

unsafe extern "C" fn trace_new_connection(db: *mut ffi::sqlite3, _error: *mut *mut std::ffi::c_char, _api: *const ffi::sqlite3_api_routines) -> c_int {
    unsafe { ffi::sqlite3_trace_v2(db, ffi::SQLITE_TRACE_STMT as c_uint, Some(on_statement), std::ptr::null_mut()) }
}

fn count_statements() {
    let status = unsafe { ffi::sqlite3_auto_extension(Some(trace_new_connection)) };
    assert_eq!(status, ffi::SQLITE_OK, "the statement counter did not register");
}

fn take_counts() -> BTreeMap<String, u64> {
    std::mem::take(&mut *STATEMENTS.lock().expect("counts"))
}

const USAGE: &str = "usage: rowbench [--rows N] [--repeats N] [--shape scalars|list|object|all] [--counts]";
const OPERATIONS: [&str; 5] = ["upsert", "read_ids", "read_query", "update_one_field", "filter_plain"];

fn spec(shape: Shape) -> ModelSpec {
    let mut fields = vec![
    FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("ownerId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("title", ScalarKind::Str),
            FieldSpec::scalar("count", ScalarKind::Int),
            FieldSpec::scalar("ratio", ScalarKind::Num),
            FieldSpec::scalar("flag", ScalarKind::Bool),
            FieldSpec::scalar("createdAt", ScalarKind::Date),
    ];
    if shape.has_list() {
        fields.push(FieldSpec::list("labels", ScalarKind::Str));
    }
    if shape.has_object() {
        fields.push(FieldSpec::object("meta").nullable());
    }
    ModelSpec::new("bench", fields)
}

fn row(index: usize, count: i64, shape: Shape) -> Map<String, Value> {
    let mut row = json!({
        "id": format!("r{index}"),
        "ownerId": format!("o{}", index % 20),
        "title": format!("title number {index}"),
        "count": count,
        "ratio": index as f64 / 7.0,
        "flag": index.is_multiple_of(2),
        "createdAt": "2026-10-14T00:00:00.000Z",
    })
    .as_object()
    .cloned()
    .expect("row is an object");
    if shape.has_list() {
        row.insert("labels".to_string(), json!(["a", "b", "c"]));
    }
    if shape.has_object() {
        row.insert("meta".to_string(), json!({"title": "t", "tags": [1, 2, 3], "inner": {"x": index}}));
    }
    row
}

fn land(db: &Db, rows: usize, count: i64, shape: Shape) {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops = (0..rows)
        .map(|index| {
            let row = row(index, count, shape);
            EntityOp::Upsert { model: ModelKey::new("bench"), id: RowId::new(&format!("r{index}")).expect("id"), row }
        })
        .collect();
    db.land(&db.admit().expect("admission"), envelope).expect("land");
}

fn run(rows: usize, shape: Shape, counted: bool) -> [f64; 5] {
    let directory = tempfile::tempdir().expect("directory");
    let db = Db::open_with(Some(&directory.path().join("bench.db")), vec![spec(shape)], None, DbConfig { cache_rows: 1 }).expect("open");
    let ids: Vec<Value> = (0..rows).map(|index| json!(format!("r{index}"))).collect();
    let mut seconds = [0.0; 5];
    let mut time = |slot: usize, work: &dyn Fn()| {
        take_counts();
        let started = Instant::now();
        work();
        seconds[slot] = started.elapsed().as_secs_f64() * 1000.0;
        if counted {
            let counts = take_counts();
            let total: u64 = counts.values().sum();
            let parts: Vec<String> = counts.iter().map(|(name, count)| format!("{name}={count}")).collect();
            println!("statements {} total={total} {}", OPERATIONS[slot], parts.join(" "));
        }
    };
    time(0, &|| land(&db, rows, 1, shape));
    time(1, &|| assert_eq!(db.query(&Query::new("bench").filtered(Where::in_list("id", ids.clone()))).expect("read").len(), rows));
    time(2, &|| assert_eq!(db.query(&Query::new("bench")).expect("query").len(), rows));
    time(3, &|| land(&db, rows, 2, shape));
    time(4, &|| assert_eq!(db.query(&Query::new("bench").filtered(Where::equal("count", json!(2)))).expect("filter").len(), rows));
    seconds
}

fn main() {
    let (mut rows, mut repeats, mut shape, mut counted) = (1000usize, 5usize, Shape::All, false);
    let mut input = std::env::args().skip(1);
    while let Some(flag) = input.next() {
        match flag.as_str() {
            "--counts" => counted = true,
            "--rows" | "--repeats" | "--shape" => {
                let value = input.next().unwrap_or_default();
                match (flag.as_str(), value.parse::<usize>().ok(), Shape::parse(&value)) {
                    ("--rows", Some(value), _) => rows = value,
                    ("--repeats", Some(value), _) => repeats = value,
                    ("--shape", _, Some(value)) => shape = value,
                    _ => {
                        eprintln!("{USAGE}");
                        std::process::exit(2);
                    }
                }
            }
            _ => {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
        }
    }
    if counted {
        count_statements();
        run(rows, shape, true);
        return;
    }
    let samples: Vec<[f64; 5]> = (0..repeats).map(|_| run(rows, shape, false)).collect();
    for (slot, name) in OPERATIONS.iter().enumerate() {
        let mut column: Vec<f64> = samples.iter().map(|sample| sample[slot]).collect();
        column.sort_by(f64::total_cmp);
        println!("{name} rows={rows} repeats={repeats} min={:.2}ms median={:.2}ms max={:.2}ms", column[0], column[column.len() / 2], column[column.len() - 1]);
    }
}
