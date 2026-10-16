#[cfg(test)]
thread_local! {
    static TOUCHED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn touch(count: usize) {
    TOUCHED.with(|touched| touched.set(touched.get() + count));
}

#[cfg(not(test))]
#[inline(always)]
pub(crate) fn touch(_count: usize) {}

#[cfg(test)]
pub(crate) fn total() -> usize {
    TOUCHED.with(std::cell::Cell::get)
}
