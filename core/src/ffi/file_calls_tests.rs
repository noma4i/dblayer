const FILE_CALLS: [&str; 6] = ["attach", "file", "read", "url", "detach", "maintain"];

#[test]
fn calls_that_touch_files_are_async_so_they_never_run_on_the_caller_thread() {
    let source = include_str!("database.rs");
    for name in FILE_CALLS {
        let declared: Vec<&str> = source.lines().filter(|line| line.trim_start().starts_with("pub ") && line.contains(&format!("fn {name}("))).collect();
        assert_eq!(declared.len(), 1, "{name}: {declared:?}");
        assert!(declared[0].contains("pub async fn"), "{name} blocks the caller: {}", declared[0]);
    }
}
