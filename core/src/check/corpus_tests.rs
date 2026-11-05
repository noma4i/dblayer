use super::corpus::*;
use super::value::*;
use serde_json::json;

fn sample() -> Trace {
    Trace {
        name: "sample".into(),
        steps: vec![
            Step { input: Input::new("open"), observed: vec![Observation::bare("opened")] },
            Step { input: Input::new("create").with("as", "r1").with("text", "a"), observed: vec![Observation::new("created", json!({"row": "r1"}))] },
            Step { input: Input::new("answer").with("op", "Feed"), observed: vec![] },
        ],
    }
}

/// Requirement: the corpus format is readable text that loses nothing. Observable: the trace after render and parse. Assert: equal.
#[test]
fn a_trace_survives_render_and_parse() {
    let trace = sample();
    assert_eq!(parse(&render(&trace)).unwrap(), trace);
}

/// Requirement: a broken corpus line is named by its number. Observable: the parse error. Assert: it names line 3.
#[test]
fn a_broken_line_is_reported_with_its_number() {
    let error = parse("{\"trace\":\"t\"}\n{\"in\":\"open\"}\n{\"in\":\n").unwrap_err();
    assert!(error.starts_with("line 3:"), "{error}");
}

/// Requirement: files are written one per trace and read back in name order. Observable: read_dir result. Assert: both traces in order.
#[test]
fn a_directory_is_written_and_read_back_in_name_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut second = sample();
    second.name = "b_second".into();
    let mut first = sample();
    first.name = "a_first".into();
    write(dir.path(), &second).unwrap();
    write(dir.path(), &first).unwrap();
    let names: Vec<String> = read_dir(dir.path()).unwrap().into_iter().map(|trace| trace.name).collect();
    assert_eq!(names, ["a_first", "b_second"]);
}
