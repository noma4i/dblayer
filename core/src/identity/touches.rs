#[cfg(test)]
thread_local! {
    static TOUCHED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static MEMBERS_READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn touch(count: usize) {
    TOUCHED.with(|touched| touched.set(touched.get() + count));
}

#[cfg(test)]
pub(crate) fn read_members(count: usize) {
    touch(count);
    MEMBERS_READ.with(|read| read.set(read.get() + count));
}

#[cfg(not(test))]
#[inline(always)]
pub(crate) fn read_members(_count: usize) {}

#[cfg(not(test))]
#[inline(always)]
pub(crate) fn touch(_count: usize) {}
