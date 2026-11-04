pub const MODEL_NAME: &str = "modelName";
pub const WIRE_LAYOUT: &str = "wireLayout";
pub const LAYOUT: &str = "layout";
pub const SYNC: &str = "sync";
pub const UPLOADS: &str = "uploads";
pub const SINGLETON: &str = "singleton";

/// A name a model cannot give to a relation, association or action. `generated` marks the members every generated model class declares itself: attributes cannot take those either.
pub struct ReservedMember {
    pub name: &'static str,
    pub generated: bool,
}

const fn runtime(name: &'static str) -> ReservedMember {
    ReservedMember { name, generated: false }
}

const fn generated(name: &'static str) -> ReservedMember {
    ReservedMember { name, generated: true }
}

pub const RESERVED_MEMBERS: [ReservedMember; 17] = [
    runtime("id"), runtime("find"), runtime("where"), runtime("save"), runtime("count"), runtime("exists"), runtime("first"), runtime("observe"), runtime("order"), runtime("retry"), runtime("discard"),
    generated(MODEL_NAME), generated(WIRE_LAYOUT), generated(LAYOUT), generated(SYNC), generated(UPLOADS), generated(SINGLETON),
];

pub fn reserved_member_names() -> Vec<&'static str> {
    RESERVED_MEMBERS.iter().map(|member| member.name).collect()
}

pub fn generated_member_names() -> Vec<&'static str> {
    RESERVED_MEMBERS.iter().filter(|member| member.generated).map(|member| member.name).collect()
}
