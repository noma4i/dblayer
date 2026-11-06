use super::Operation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueueOperation {
    pub operation: Operation,
    pub uploading: bool,
}
