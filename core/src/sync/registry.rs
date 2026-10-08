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

    pub fn paged(self, page_size: u32) -> Self {
        Self { page_size, ..self }
    }
}
