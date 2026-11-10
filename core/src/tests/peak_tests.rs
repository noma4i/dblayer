use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

const TAG: usize = std::mem::size_of::<u64>();

struct Counting;

#[global_allocator]
static ALLOCATOR: Counting = Counting;
static MEASUREMENTS: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static MEASUREMENT: Cell<u64> = const { Cell::new(0) };
    static LIVE: Cell<usize> = const { Cell::new(0) };
    static PEAK: Cell<usize> = const { Cell::new(0) };
}

fn tagged(layout: Layout) -> Layout {
    Layout::from_size_align(layout.size() + TAG, layout.align()).expect("a block with its tag fits the address space")
}

unsafe fn mark(block: *mut u8, size: usize) {
    let measurement = MEASUREMENT.get();
    unsafe { block.add(size).cast::<u64>().write_unaligned(measurement) };
    if measurement == 0 { return; }
    let live = LIVE.get() + size;
    LIVE.set(live);
    if live > PEAK.get() { PEAK.set(live); }
}

unsafe fn unmark(block: *mut u8, size: usize) {
    let owner = unsafe { block.add(size).cast::<u64>().read_unaligned() };
    if owner != 0 && owner == MEASUREMENT.get() { LIVE.set(LIVE.get() - size); }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let block = unsafe { System.alloc(tagged(layout)) };
        if !block.is_null() { unsafe { mark(block, layout.size()) }; }
        block
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let block = unsafe { System.alloc_zeroed(tagged(layout)) };
        if !block.is_null() { unsafe { mark(block, layout.size()) }; }
        block
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        unsafe { unmark(block, layout.size()) };
        unsafe { System.dealloc(block, tagged(layout)) };
    }

    unsafe fn realloc(&self, block: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        unsafe { unmark(block, layout.size()) };
        let next = unsafe { System.realloc(block, tagged(layout), size + TAG) };
        if next.is_null() {
            unsafe { mark(block, layout.size()) };
        } else {
            unsafe { mark(next, size) };
        }
        next
    }
}

/// Peak of the bytes the calling thread allocates inside the scenario and still holds. Blocks from before the scenario and from other threads do not count, in either direction.
pub(crate) fn measure_peak<T>(scenario: impl FnOnce() -> T) -> (T, usize) {
    assert_eq!(MEASUREMENT.get(), 0, "one measurement per thread at a time");
    LIVE.set(0);
    PEAK.set(0);
    MEASUREMENT.set(MEASUREMENTS.fetch_add(1, Ordering::Relaxed) + 1);
    let result = scenario();
    MEASUREMENT.set(0);
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

#[test]
fn measure_peak_ignores_the_release_of_older_blocks() {
    const BLOCK: usize = 4 * 1024 * 1024;
    let older = vec![1u8; BLOCK];
    let (kept, peak) = measure_peak(move || {
        drop(older);
        vec![2u8; BLOCK]
    });
    assert!(peak >= BLOCK, "releasing an older block does not hide a new one: {peak}");
    drop(kept);
}
