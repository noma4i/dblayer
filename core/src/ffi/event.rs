use super::wire::WireRow;

#[derive(Clone, Debug, uniffi::Record)]
pub struct WireModelEvent {
    pub area: String,
    pub kind: String,
    pub id: String,
    pub before: Option<WireRow>,
    pub after: Option<WireRow>,
}
