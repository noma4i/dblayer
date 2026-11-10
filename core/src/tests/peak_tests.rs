use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Counting;

#[global_allocator]
static ALLOCATOR: Counting = Counting;

thread_local! {
    static MEASURED: Cell<bool> = const { Cell::new(false) };
    static LIVE: Cell<usize> = const { Cell::new(0) };
    static PEAK: Cell<usize> = const { Cell::new(0) };
}

fn grown(size: usize) {
    if !MEASURED.get() { return; }
    let live = LIVE.get() + size;
    LIVE.set(live);
    if live > PEAK.get() { PEAK.set(live); }
}

fn shrunk(size: usize) {
    if MEASURED.get() { LIVE.set(LIVE.get().saturating_sub(size)); }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let block = unsafe { System.alloc(layout) };
        if !block.is_null() { grown(layout.size()); }
        block
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let block = unsafe { System.alloc_zeroed(layout) };
        if !block.is_null() { grown(layout.size()); }
        block
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        shrunk(layout.size());
        unsafe { System.dealloc(block, layout) };
    }

    unsafe fn realloc(&self, block: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(block, layout, size) };
        if !next.is_null() {
            shrunk(layout.size());
            grown(size);
        }
        next
    }
}

/// Peak of the bytes the calling thread holds above its level at the start of the scenario.
pub(crate) fn measure_peak<T>(scenario: impl FnOnce() -> T) -> (T, usize) {
    assert!(!MEASURED.get(), "one measurement per thread at a time");
    LIVE.set(0);
    PEAK.set(0);
    MEASURED.set(true);
    let result = scenario();
    MEASURED.set(false);
    (result, PEAK.get())
}

#[test]
fn measure_peak_reports_held_and_released_bytes() {
    const BLOCK: usize = 16 * 1024 * 1024;
    let (kept, held) = measure_peak(|| vec![7u8; BLOCK]);
    assert!(held >= BLOCK, "a held block of {BLOCK} bytes shows in the peak: {held}");
    drop(kept);
    let (_, churn) = measure_peak(|| {
        for round in 0..64u8 {
            drop(vec![round; BLOCK / 64]);
        }
    });
    assert!((BLOCK / 64..BLOCK / 32).contains(&churn), "64 blocks freed one by one peak at one block: {churn}");
}
