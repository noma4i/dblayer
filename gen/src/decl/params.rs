use crate::parser::{Node, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParamBase {
    Bool,
    Int,
    Long,
    Num,
    String,
    Id,
    Named(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParamType {
    pub base: ParamBase,
    pub list: bool,
    pub null_items: bool,
}

pub(crate) const PARAM_TYPES: &str = "parameter type must be :bool, :int, :long, :num, :string, :id, the name of a schema input object or enum like QrCodeInput, [:id], or a list of schema inputs or enums like [WidgetInput]";

impl ParamType {
    pub fn scalar(base: ParamBase) -> Self {
        Self { base, list: false, null_items: false }
    }

    pub fn describe(&self) -> String {
        let kind = self.kind_text(false);
        if matches!(self.base, ParamBase::Named(_)) || self.list { kind } else { format!(":{kind}") }
    }

    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Symbol(name) => scalar_base(name).map(Self::scalar),
            Value::Const(name) => Some(Self::scalar(ParamBase::Named(name.clone()))),
            Value::Array(items) => match items.as_slice() {
                [Node { item: Value::Symbol(name), .. }] if name == "id" => Some(Self { base: ParamBase::Id, list: true, null_items: false }),
                [Node { item: Value::Const(name), .. }] => Some(Self { base: ParamBase::Named(name.clone()), list: true, null_items: false }),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn kind_text(&self, null: bool) -> String {
        let base = match (&self.base, self.list) {
            (ParamBase::Id, true) => "ids".to_string(),
            (base, list) => {
                let name = match base {
                    ParamBase::Bool => "bool".to_string(),
                    ParamBase::Int => "int".to_string(),
                    ParamBase::Long => "long".to_string(),
                    ParamBase::Num => "num".to_string(),
                    ParamBase::String => "string".to_string(),
                    ParamBase::Id => "id".to_string(),
                    ParamBase::Named(name) => name.clone(),
                };
                if list { format!("[{name}{}]", if self.null_items { "?" } else { "" }) } else { name }
            }
        };
        if null { format!("{base}?") } else { base }
    }
}

fn scalar_base(name: &str) -> Option<ParamBase> {
    Some(match name {
        "bool" => ParamBase::Bool,
        "int" => ParamBase::Int,
        "long" => ParamBase::Long,
        "num" => ParamBase::Num,
        "string" => ParamBase::String,
        "id" => ParamBase::Id,
        _ => return None,
    })
}
