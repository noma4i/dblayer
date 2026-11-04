use tokio::runtime::{Handle, RuntimeFlavor};

pub(crate) fn blocking_unit<T>(unit: impl FnOnce() -> T) -> T {
    match Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread && tokio::task::try_id().is_some() => tokio::task::block_in_place(unit),
        _ => unit(),
    }
}
