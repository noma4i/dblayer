use dblayer::schema::{FieldKind, FieldSpec, ModelSpec};

const SWIFT_KEYWORDS: &[&str] = &[
    "as", "break", "case", "class", "continue", "default", "defer", "do", "else", "enum", "for", "func", "guard", "if", "import", "in", "is", "let", "return", "switch", "var", "where", "while", "extension",
    "associatedtype", "catch", "deinit", "false", "fileprivate", "inout", "internal", "nil", "operator", "private", "protocol", "public", "rethrows", "self", "Self", "static", "struct", "subscript", "super", "throw", "throws", "true", "try", "typealias",
];
const KOTLIN_KEYWORDS: &[&str] = &[
    "as", "break", "class", "continue", "do", "else", "false", "for", "fun", "if", "in", "interface", "is", "null", "object", "package", "return", "super", "this", "throw", "true", "try", "typealias", "typeof", "val", "var", "when", "while",
];
const SWIFT_REJECTED: [&str; 5] = ["self", "Type", "Protocol", "init", "unknown"];
const KOTLIN_REJECTED: [&str; 2] = ["Unknown", "Companion"];

pub(crate) fn identifier(name: &str, swift: bool) -> String {
    let keywords = if swift { SWIFT_KEYWORDS } else { KOTLIN_KEYWORDS };
    if keywords.contains(&name) { format!("`{name}`") } else { name.to_string() }
}

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
                identifier(&case, swift)
            })
            .collect()
    }

    pub fn check(&self) -> Result<(), String> {
        self.check_as("enum", "value")
    }

    pub fn check_as(&self, noun: &str, item: &str) -> Result<(), String> {
        let name = &self.name;
        if self.values.is_empty() {
            return Err(format!("{noun} `{name}` has no {item}s"));
        }
        for swift in [true, false] {
            let platform = if swift { "Swift" } else { "Kotlin" };
            let mut seen: Vec<(String, &str)> = Vec::new();
            for value in &self.values {
                let case = plain_case(value, swift);
                if case.is_empty() || case.starts_with(|character: char| character.is_ascii_digit()) {
                    return Err(format!("{noun} `{name}`: {item} `{value}` cannot become a {platform} case name"));
                }
                if (swift && SWIFT_REJECTED.contains(&case.as_str())) || (!swift && KOTLIN_REJECTED.contains(&case.as_str())) {
                    return Err(format!("{noun} `{name}`: {item} `{value}` becomes the reserved {platform} case `{case}`"));
                }
                if let Some((_, other)) = seen.iter().find(|(known, _)| *known == case) {
                    return Err(format!("{noun} `{name}`: {item}s `{other}` and `{value}` both become the {platform} case `{case}`"));
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Site {
    pub decl: EnumDecl,
    pub model: String,
    pub scope: Option<String>,
    pub path: Vec<String>,
}

fn declared(field: &FieldSpec) -> Option<EnumDecl> {
    match (&field.kind, &field.app_type) {
        (FieldKind::Enum { values }, Some(name)) => Some(EnumDecl { name: name.clone(), values: values.clone() }),
        (FieldKind::List { .. }, Some(name)) if !field.values.is_empty() => Some(EnumDecl { name: name.clone(), values: field.values.clone() }),
        _ => None,
    }
}

fn collect(field: &FieldSpec, label: &str, path: &mut Vec<String>, found: &mut Vec<(EnumDecl, Vec<String>)>) {
    path.push(label.to_string());
    found.extend(declared(field).map(|decl| (decl, path.clone())));
    let variants = matches!(field.kind, FieldKind::Union | FieldKind::UnionArray);
    for inner in &field.fields {
        collect(inner, if variants { inner.app_type.as_deref().unwrap_or(&inner.name) } else { &inner.name }, path, found);
    }
    for inner in &field.shared {
        collect(inner, &inner.name, path, found);
    }
    path.pop();
}

pub fn declared_in_field(field: &FieldSpec) -> Vec<EnumDecl> {
    let mut found = Vec::new();
    collect(field, &field.name, &mut Vec::new(), &mut found);
    found.into_iter().map(|(decl, _)| decl).collect()
}

pub fn declared_in(models: &[ModelSpec]) -> Vec<Site> {
    let mut sites = Vec::new();
    for model in models {
        let own = model.fields.iter().chain(model.ephemeral.iter().map(|field| &field.field)).map(|field| (None, field));
        let edges = model.relations.iter().flat_map(|relation| relation.edge_fields.iter().map(|field| (Some(relation.name.clone()), field)));
        for (scope, field) in own.chain(edges) {
            let mut found = Vec::new();
            collect(field, &field.name, &mut Vec::new(), &mut found);
            sites.extend(found.into_iter().map(|(decl, path)| Site { decl, model: model.key.as_str().to_string(), scope: scope.clone(), path }));
        }
    }
    sites
}
