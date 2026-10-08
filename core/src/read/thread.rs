use std::cmp::Ordering;

pub const THREAD_ORDER_FIELDS: [&str; 2] = ["sequenceNumber", "createdAt"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadRow {
    pub id: String,
    pub sequence_number: Option<i64>,
    pub created_at: String,
    pub is_temp: bool,
}

impl ThreadRow {
    pub fn new(id: &str) -> Self {
        Self { id: id.to_string(), sequence_number: None, created_at: String::new(), is_temp: false }
    }

    pub fn sequence(self, sequence_number: i64) -> Self {
        Self { sequence_number: Some(sequence_number), ..self }
    }

    pub fn created(self, created_at: &str) -> Self {
        Self { created_at: created_at.to_string(), ..self }
    }

    pub fn temp(self) -> Self {
        Self { is_temp: true, ..self }
    }

    pub fn is_before(&self, other: &ThreadRow) -> bool {
        if let (Some(left), Some(right)) = (self.sequence_number, other.sequence_number) {
            if left != right {
                return left > right;
            }
            if self.is_temp != other.is_temp {
                return self.is_temp;
            }
        }
        if self.created_at != other.created_at {
            return self.created_at > other.created_at;
        }
        if self.sequence_number.is_some() != other.sequence_number.is_some() {
            return self.sequence_number.is_some();
        }
        self.id > other.id
    }

    pub fn sorted(mut rows: Vec<ThreadRow>) -> Vec<ThreadRow> {
        rows.sort_by(|left, right| {
            if left.is_before(right) {
                Ordering::Less
            } else if right.is_before(left) {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });
        rows
    }

    pub fn max_sequence_number(rows: &[ThreadRow]) -> Option<i64> {
        rows.iter().filter(|row| !row.is_temp).filter_map(|row| row.sequence_number).max()
    }
}
