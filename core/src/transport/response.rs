#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseShape {
    DataOnly,
    DataWithErrors,
    ErrorsOnly,
    Thrown,
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseOutcome {
    Commit,
    MarkFailed,
}

impl ResponseShape {
    pub fn of(has_data: bool, error_count: usize, did_throw: bool) -> ResponseShape {
        if did_throw {
            return ResponseShape::Thrown;
        }
        match (has_data, error_count > 0) {
            (true, false) => ResponseShape::DataOnly,
            (true, true) => ResponseShape::DataWithErrors,
            (false, true) => ResponseShape::ErrorsOnly,
            (false, false) => ResponseShape::Empty,
        }
    }

    pub fn outcome(self) -> ResponseOutcome {
        match self {
            ResponseShape::DataOnly => ResponseOutcome::Commit,
            ResponseShape::DataWithErrors | ResponseShape::ErrorsOnly | ResponseShape::Empty | ResponseShape::Thrown => {
                ResponseOutcome::MarkFailed
            }
        }
    }
}
