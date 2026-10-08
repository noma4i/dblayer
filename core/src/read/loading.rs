#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LoadingPhase {
    #[default]
    Idle,
    InitialLoading,
    Ready,
    Refreshing,
    LoadingMore,
    Error,
}

impl LoadingPhase {
    pub fn next(from: LoadingPhase, has_data: bool) -> LoadingPhase {
        match (from, has_data) {
            (LoadingPhase::Idle, true) => LoadingPhase::Ready,
            (LoadingPhase::Idle, false) => LoadingPhase::InitialLoading,
            (phase, _) => phase,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LoadingState {
    pub phase: LoadingPhase,
    pub has_data: bool,
    pub is_offline: bool,
    pub retry_attempt: u32,
}

impl LoadingState {
    pub const LOCAL: LoadingState =
        LoadingState { phase: LoadingPhase::Ready, has_data: false, is_offline: false, retry_attempt: 0 };

    pub fn show_skeleton(&self) -> bool {
        self.phase == LoadingPhase::InitialLoading && !self.has_data
    }

    pub fn show_empty_state(&self) -> bool {
        self.phase == LoadingPhase::Ready && !self.has_data
    }

    pub fn show_footer_spinner(&self) -> bool {
        self.phase == LoadingPhase::LoadingMore
    }

    pub fn show_error_banner(&self) -> bool {
        self.phase == LoadingPhase::Error
    }

    pub fn is_retrying(&self) -> bool {
        self.retry_attempt > 0
    }

    pub fn with_phase(self, phase: LoadingPhase) -> Self {
        Self { phase, ..self }
    }
}
