pub const DEFAULT_PAGE_SIZE: u32 = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationDef {
    pub name: String,
    pub document: String,
    pub root_path: String,
    pub page_size: u32,
}

impl OperationDef {
    pub fn new(name: &str, document: &str, root_path: &str) -> Self {
        Self { name: name.to_string(), document: document.to_string(), root_path: root_path.to_string(), page_size: DEFAULT_PAGE_SIZE }
    }

    /// Builds a definition whose root path is the response key of the document's first field.
    pub fn from_document(name: &str, document: &str) -> Option<Self> {
        Some(Self::new(name, document, &first_field(document)?))
    }

    pub fn paged(self, page_size: u32) -> Self {
        Self { page_size, ..self }
    }
}

fn first_field(document: &str) -> Option<String> {
    let mut depth = 0usize;
    let mut body = None;
    for (index, character) in document.char_indices() {
        match character {
            '(' | '[' => depth += 1,
            ')' | ']' => depth = depth.saturating_sub(1),
            '{' if depth == 0 => {
                body = Some(&document[index + 1..]);
                break;
            }
            _ => {}
        }
    }
    let body = body?.trim_start();
    let end = body.find(|character: char| !(character.is_alphanumeric() || character == '_')).unwrap_or(body.len());
    (end > 0).then(|| body[..end].to_string())
}
