pub(crate) fn upper_first(text: &str) -> String {
    let mut chars = text.chars();
    chars.next().map(|first| first.to_uppercase().collect::<String>() + chars.as_str()).unwrap_or_default()
}

pub(crate) fn lower_first(text: &str) -> String {
    let mut chars = text.chars();
    chars.next().map(|first| first.to_lowercase().collect::<String>() + chars.as_str()).unwrap_or_default()
}

pub(crate) fn camel(text: &str) -> String {
    text.split(['_', '-']).filter(|part| !part.is_empty()).map(upper_first).collect()
}

pub(crate) fn member(text: &str) -> String {
    lower_first(&camel(text))
}
