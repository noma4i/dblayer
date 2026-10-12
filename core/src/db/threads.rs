use super::DbError;

#[cfg(test)]
thread_local! {
    static REFUSED: std::cell::RefCell<Option<&'static str>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn refuse_spawn_here(name: &'static str) {
    REFUSED.with(|refused| *refused.borrow_mut() = Some(name));
}

pub(crate) fn spawn_named(name: &'static str, body: impl FnOnce() + Send + 'static) -> Result<(), DbError> {
    #[cfg(test)]
    if REFUSED.with(|refused| *refused.borrow() == Some(name)) {
        return Err(DbError::ThreadSpawn { name, detail: "refused by the test".into() });
    }
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(body)
        .map(drop)
        .map_err(|error| DbError::ThreadSpawn { name, detail: error.to_string() })
}
