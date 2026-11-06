pub fn selected_policy<'a>(is_empty_result: bool, landed: &'a str, empty: Option<&'a str>) -> &'a str {
    match empty {
        Some(empty) if is_empty_result => empty,
        _ => landed,
    }
}
