const SWIFT_KEYWORDS: [&str; 24] = [
    "as", "break", "case", "class", "continue", "default", "defer", "do", "else", "enum", "for", "func", "guard", "if", "import", "in", "is", "let", "return", "switch", "var", "where", "while", "extension",
];
const SWIFT_REJECTED: [&str; 5] = ["self", "Type", "Protocol", "init", "unknown"];
const KOTLIN_REJECTED: [&str; 2] = ["Unknown", "Companion"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumDecl {
    pub name: String,
    pub values: Vec<String>,
}

fn parts(value: &str) -> Vec<String> {
    let screaming = !value.chars().any(char::is_lowercase);
    value
        .split(|character: char| !character.is_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(|part| if screaming { part.to_lowercase() } else { part.to_string() })
        .collect()
}

fn upper_first(text: &str) -> String {
    let mut characters = text.chars();
    characters.next().map(|first| first.to_uppercase().collect::<String>() + characters.as_str()).unwrap_or_default()
}

fn lower_first(text: &str) -> String {
    let mut characters = text.chars();
    characters.next().map(|first| first.to_lowercase().collect::<String>() + characters.as_str()).unwrap_or_default()
}

fn plain_case(value: &str, swift: bool) -> String {
    let words = parts(value);
    words
        .iter()
        .enumerate()
        .map(|(at, word)| if swift && at == 0 { lower_first(word) } else { upper_first(word) })
        .collect()
}

impl EnumDecl {
    pub fn new(name: &str, values: &[String]) -> Self {
        Self { name: name.to_string(), values: values.to_vec() }
    }

    /// Case names in the language convention: lowerCamel in Swift (backticked when a keyword), PascalCase in Kotlin.
    pub fn cases(&self, swift: bool) -> Vec<String> {
        self.values
            .iter()
            .map(|value| {
                let case = plain_case(value, swift);
                if swift && SWIFT_KEYWORDS.contains(&case.as_str()) { format!("`{case}`") } else { case }
            })
            .collect()
    }

    pub fn check(&self) -> Result<(), String> {
        let name = &self.name;
        if self.values.is_empty() {
            return Err(format!("enum `{name}` has no values"));
        }
        for swift in [true, false] {
            let platform = if swift { "Swift" } else { "Kotlin" };
            let mut seen: Vec<(String, &str)> = Vec::new();
            for value in &self.values {
                let case = plain_case(value, swift);
                if case.is_empty() || case.starts_with(|character: char| character.is_ascii_digit()) {
                    return Err(format!("enum `{name}`: value `{value}` cannot become a {platform} case name"));
                }
                if (swift && SWIFT_REJECTED.contains(&case.as_str())) || (!swift && KOTLIN_REJECTED.contains(&case.as_str())) {
                    return Err(format!("enum `{name}`: value `{value}` becomes the reserved {platform} case `{case}`"));
                }
                if let Some((_, other)) = seen.iter().find(|(known, _)| *known == case) {
                    return Err(format!("enum `{name}`: values `{other}` and `{value}` both become the {platform} case `{case}`"));
                }
                seen.push((case, value));
            }
        }
        Ok(())
    }
}

pub fn merge_enums(found: impl IntoIterator<Item = EnumDecl>) -> Result<Vec<EnumDecl>, String> {
    let mut merged: Vec<EnumDecl> = Vec::new();
    for declared in found {
        match merged.iter().find(|known| known.name == declared.name) {
            Some(known) if known.values != declared.values => return Err(format!("enum `{}` is declared with different values: {} and {}", declared.name, known.values.join(", "), declared.values.join(", "))),
            Some(_) => {}
            None => merged.push(declared),
        }
    }
    Ok(merged)
}
