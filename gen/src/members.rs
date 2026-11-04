#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Platform {
    Swift,
    Kotlin,
}

impl Platform {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Platform::Swift => "Swift",
            Platform::Kotlin => "Kotlin",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Redeclared {
    pub outer: String,
    pub path: String,
    pub member: String,
    pub shown: String,
    pub kind: String,
}

const MODIFIERS: &[&str] = &[
    "public", "internal", "private", "fileprivate", "protected", "open", "final", "override", "static", "lazy", "mutating", "nonisolated", "convenience", "required", "indirect", "suspend", "inline", "operator", "infix", "tailrec",
    "abstract", "const", "lateinit", "external", "data", "sealed", "value", "inner", "annotation",
];

struct Container {
    indent: usize,
    name: String,
}

struct Declaration {
    name: String,
    is_static: bool,
    function: bool,
    signature: String,
    receiver: Option<String>,
}

fn strip_attributes(text: &str) -> &str {
    let mut rest = text.trim_start();
    while let Some(after) = rest.strip_prefix('@') {
        let end = after.find(|character: char| !(character.is_alphanumeric() || character == '_' || character == ':')).unwrap_or(after.len());
        rest = after[end..].trim_start();
        if rest.starts_with('(') {
            rest = rest[balanced_end(rest, 0).map_or(rest.len(), |close| close + 1)..].trim_start();
        }
    }
    rest
}

fn balanced_end(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut quoted = false;
    let mut index = open;
    while index < bytes.len() {
        let byte = bytes[index];
        if quoted {
            if byte == b'\\' { index += 1; } else if byte == b'"' { quoted = false; }
        } else {
            match byte {
                b'"' => quoted = true,
                b'(' | b'[' | b'{' => depth += 1,
                b'<' => depth += 1,
                b'>' if index > 0 && bytes[index - 1] == b'-' => {}
                b')' | b']' | b'}' | b'>' => {
                    depth -= 1;
                    if depth == 0 { return Some(index); }
                }
                _ => {}
            }
        }
        index += 1;
    }
    None
}

fn split_top_level(text: &str, separator: u8) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut parts = Vec::new();
    let (mut depth, mut start, mut quoted) = (0i32, 0usize, false);
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if quoted {
            if byte == b'\\' { index += 1; } else if byte == b'"' { quoted = false; }
        } else {
            match byte {
                b'"' => quoted = true,
                b'(' | b'[' | b'{' | b'<' => depth += 1,
                b'>' if index > 0 && bytes[index - 1] == b'-' => {}
                b')' | b']' | b'}' | b'>' => depth -= 1,
                _ if byte == separator && depth == 0 => {
                    parts.push(&text[start..index]);
                    start = index + 1;
                }
                _ => {}
            }
        }
        index += 1;
    }
    parts.push(&text[start..]);
    parts
}

fn parameter_identity(parameter: &str, platform: Platform) -> String {
    let parts = split_top_level(parameter, b':');
    let Some((head, rest)) = parts.split_first() else { return String::new() };
    let declared = split_top_level(&rest.join(":"), b'=')[0].split_whitespace().collect::<String>();
    match platform {
        Platform::Swift => format!("{}:{declared}", head.split_whitespace().next().unwrap_or_default()),
        Platform::Kotlin => declared,
    }
}

fn identifier_end(text: &str) -> usize {
    text.find(|character: char| !(character.is_alphanumeric() || character == '_' || character == '`' || character == '.')).unwrap_or(text.len())
}

fn member_name(text: &str) -> String {
    let raw = &text[..identifier_end(text)];
    raw.rsplit('.').next().unwrap_or_default().trim_matches('`').to_string()
}

fn declaration(lines: &[&str], platform: Platform) -> Option<Declaration> {
    let mut rest = strip_attributes(lines[0]);
    let mut is_static = false;
    loop {
        rest = strip_attributes(rest);
        let word = rest.split_whitespace().next().unwrap_or_default();
        if word == "static" { is_static = true; }
        let access_scope = word.starts_with("private(") || word.starts_with("fileprivate(");
        if MODIFIERS.contains(&word) || access_scope { rest = rest[word.len()..].trim_start(); } else { break; }
    }
    let word = rest.split_whitespace().next().unwrap_or_default();
    let initializer = word.strip_prefix("init").filter(|after| after.starts_with(['(', '?', '<']));
    let (function, tail) = match word {
        "let" | "var" | "val" => (false, rest[word.len()..].trim_start()),
        "func" | "fun" => (true, rest[word.len()..].trim_start()),
        _ if initializer.is_some() => (true, rest),
        _ => return None,
    };
    let tail = if function && tail.starts_with('<') { tail[balanced_end(tail, 0).map_or(0, |close| close + 1)..].trim_start() } else { tail };
    let name = if initializer.is_some() { "init".to_string() } else { member_name(tail) };
    if name.is_empty() { return None; }
    let signature = if function {
        let joined = lines.iter().take(40).map(|line| line.trim()).collect::<Vec<_>>().join(" ");
        let from = strip_attributes(&joined);
        let open = from.find('(')?;
        let close = balanced_end(from, open)?;
        split_top_level(&from[open + 1..close], b',').iter().filter(|part| !part.trim().is_empty()).map(|part| parameter_identity(part.trim(), platform)).collect::<Vec<_>>().join(",")
    } else {
        String::new()
    };
    let receiver = tail[..identifier_end(tail)].rsplit_once('.').map(|(receiver, _)| receiver.replace('`', ""));
    Some(Declaration { name, is_static, function, signature, receiver })
}

fn container(line: &str) -> Option<(String, bool)> {
    let trimmed = line.trim_end();
    if !(trimmed.ends_with('{') || trimmed.ends_with('(')) { return None; }
    let mut rest = strip_attributes(line);
    loop {
        let word = rest.split_whitespace().next().unwrap_or_default();
        if MODIFIERS.contains(&word) || word.starts_with("private(") { rest = rest[word.len()..].trim_start(); } else { break; }
    }
    let mut words = rest.split_whitespace();
    let word = words.next()?;
    if word == "companion" { return Some(("companion".to_string(), false)); }
    let word = if word == "enum" && rest.split_whitespace().nth(1) == Some("class") { words.next(); "class" } else { word };
    if !["struct", "enum", "class", "extension", "object", "interface", "protocol", "actor"].contains(&word) { return None; }
    let named = words.next().map(|next| member_name(next)).filter(|name| !name.is_empty());
    Some(named.map_or_else(|| (word.to_string(), true), |name| (name, false)))
}

fn describe(declaration: &Declaration) -> String {
    format!("{}{}", if declaration.is_static { "static " } else { "" }, if declaration.function { "function" } else { "property" })
}

pub(crate) fn redeclared(source: &str, platform: Platform) -> Vec<Redeclared> {
    let lines: Vec<&str> = source.lines().collect();
    let mut stack: Vec<Container> = Vec::new();
    let mut seen: std::collections::HashSet<(String, bool, bool, String, String)> = std::collections::HashSet::new();
    let mut found = Vec::new();
    let mut raw_text = false;
    for (at, line) in lines.iter().enumerate() {
        let quotes = line.matches("\"\"\"").count();
        let inside_text = raw_text;
        if quotes % 2 == 1 { raw_text = !raw_text; }
        if inside_text || line.trim().is_empty() { continue; }
        let indent = line.len() - line.trim_start().len();
        let trimmed = line.trim_start();
        let continues_header = trimmed.starts_with(')');
        while stack.last().is_some_and(|top| top.indent > indent) { stack.pop(); }
        if stack.last().is_some_and(|top| top.indent == indent) && !continues_header {
            stack.pop();
            if trimmed.starts_with('}') { continue; }
        }
        if continues_header { continue; }
        if let Some((name, anonymous)) = container(line) {
            stack.push(Container { indent, name: if anonymous { format!("{name} at line {}", at + 1) } else { name } });
            continue;
        }
        let (outer, path) = match stack.last() {
            Some(top) if indent == top.indent + 4 => (stack[0].name.clone(), stack.iter().map(|open| open.name.as_str()).collect::<Vec<_>>().join(" > ")),
            Some(_) => continue,
            None if indent == 0 => match declaration(&lines[at..], platform).and_then(|member| member.receiver) {
                Some(receiver) => (receiver.clone(), receiver),
                None => continue,
            },
            None => continue,
        };
        let Some(member) = declaration(&lines[at..], platform) else { continue };
        let key = (path.clone(), member.is_static, member.function, member.name.clone(), member.signature.clone());
        if seen.insert(key) { continue; }
        let kind = describe(&member);
        let shown = if member.function { format!("{}({})", member.name, member.signature) } else { member.name.clone() };
        found.push(Redeclared { outer, path, member: member.name.clone(), shown, kind });
    }
    found
}

pub(crate) fn declared_names(model: &dblayer::schema::ModelSpec, member: &str) -> Vec<String> {
    let prints = |source: &str| {
        let printed = crate::emit::name(source);
        printed == member || format!("{printed}Variables") == member || source == member
    };
    let relations = model.relations.iter().filter(|relation| prints(&relation.name)).map(|relation| format!("scope `{}`", relation.name));
    let associations = model.associations.iter().filter(|association| prints(&association.name)).map(|association| format!("association `{}`", association.name));
    let actions = model.actions.iter().filter(|action| prints(&action.name)).map(|action| format!("operation `{}`", action.name));
    let fields = model.fields.iter().filter(|field| field.name == member).map(|field| format!("attribute `{}`", field.name));
    relations.chain(associations).chain(actions).chain(fields).collect()
}
