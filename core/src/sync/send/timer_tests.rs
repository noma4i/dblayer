#[test]
fn inv_wait_03_the_send_sources_hold_no_timer() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/sync/send");
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if name.ends_with("_tests.rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path).unwrap();
        for word in ["sleep(", "interval(", "sleep_until("] {
            assert!(!source.contains(word), "{name} holds a timer: {word}");
        }
    }
}

#[test]
fn inv_wait_04_no_test_of_the_wrappers_waits_without_a_bound() {
    let scripts = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../scripts");
    for script in ["check_waits_test.sh", "check_waits.sh"] {
        let output = std::process::Command::new("bash").arg(scripts.join(script)).output().unwrap();
        assert!(output.status.success(), "{script}: {}", String::from_utf8_lossy(&output.stderr));
    }
}
