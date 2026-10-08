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
    MarkDeliveryUnknown,
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
            ResponseShape::DataWithErrors | ResponseShape::ErrorsOnly | ResponseShape::Empty => {
                ResponseOutcome::MarkFailed
            }
            ResponseShape::Thrown => ResponseOutcome::MarkDeliveryUnknown,
        }
    }
}

impl ResponseOutcome {
    pub fn allows_retry(self) -> bool {
        self == ResponseOutcome::MarkFailed
    }
}
