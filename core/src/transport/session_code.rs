#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum SessionCode {
    SequenceConflict,
    ReplayedRequest,
    RateLimited,
    AccountUnavailable,
    Unauthenticated,
    SessionEnded,
    InvalidCode,
    InvalidPhone,
    InvalidProvider,
    ProviderRejected,
    AlreadyAuthenticated,
    ServerKeyMismatch,
    Other,
}

const NAMES: [(&str, SessionCode); 13] = [
    ("SEQUENCE_CONFLICT", SessionCode::SequenceConflict),
    ("REPLAYED_REQUEST", SessionCode::ReplayedRequest),
    ("RATE_LIMITED", SessionCode::RateLimited),
    ("ACCOUNT_UNAVAILABLE", SessionCode::AccountUnavailable),
    ("UNAUTHENTICATED", SessionCode::Unauthenticated),
    ("SESSION_ENDED", SessionCode::SessionEnded),
    ("INVALID_CODE", SessionCode::InvalidCode),
    ("INVALID_PHONE", SessionCode::InvalidPhone),
    ("INVALID_PROVIDER", SessionCode::InvalidProvider),
    ("PROVIDER_REJECTED", SessionCode::ProviderRejected),
    ("ALREADY_AUTHENTICATED", SessionCode::AlreadyAuthenticated),
    ("SERVER_KEY_MISMATCH", SessionCode::ServerKeyMismatch),
    ("OTHER", SessionCode::Other),
];

impl SessionCode {
    pub fn name(self) -> &'static str {
        NAMES.iter().find(|(_, code)| *code == self).map_or("OTHER", |(name, _)| name)
    }

    pub fn named(name: &str) -> Option<Self> {
        NAMES.iter().find(|(known, _)| *known == name).map(|(_, code)| *code)
    }

    pub fn from_wire(code: &str) -> Option<Self> {
        Self::named(code).filter(|code| !matches!(code, Self::ServerKeyMismatch | Self::Other))
    }

    pub fn of(code: &str) -> Self {
        Self::named(code).unwrap_or(Self::Other)
    }
}
