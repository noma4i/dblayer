use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnceVerdict {
    Run,
    SkipCommittedKey,
    SkipOpenOperation,
    MissingKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OnceRequest {
    pub namespace: String,
    pub dedupe_key: Option<String>,
}

impl OnceRequest {
    pub fn new(namespace: &str, dedupe_key: Option<&str>) -> Self {
        Self { namespace: namespace.to_string(), dedupe_key: dedupe_key.map(String::from) }
    }

    fn has_key(&self) -> bool {
        self.dedupe_key.as_deref().is_some_and(|key| !key.is_empty())
    }
}

pub fn once_verdict(
    request: &OnceRequest,
    declares_once: bool,
    committed_keys: &HashSet<OnceRequest>,
    open_keys: &HashSet<OnceRequest>,
) -> OnceVerdict {
    if !declares_once {
        return OnceVerdict::Run;
    }
    if !request.has_key() {
        return OnceVerdict::MissingKey;
    }
    if committed_keys.contains(request) {
        return OnceVerdict::SkipCommittedKey;
    }
    if open_keys.contains(request) {
        return OnceVerdict::SkipOpenOperation;
    }
    OnceVerdict::Run
}

pub fn calls_transport(verdict: OnceVerdict) -> bool {
    verdict == OnceVerdict::Run
}

pub fn committing(committed_keys: &HashSet<OnceRequest>, request: &OnceRequest, verdict: OnceVerdict) -> HashSet<OnceRequest> {
    let mut keys = committed_keys.clone();
    if verdict == OnceVerdict::Run && request.has_key() {
        keys.insert(request.clone());
    }
    keys
}
