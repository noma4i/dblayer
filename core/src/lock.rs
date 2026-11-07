pub(crate) fn acquire<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.try_lock() {
        Ok(guard) => guard,
        Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
        Err(std::sync::TryLockError::WouldBlock) => crate::blocking::blocking_unit(|| mutex.lock().unwrap_or_else(|error| error.into_inner())),
    }
}

#[cfg(not(any(debug_assertions, test)))]
use std::sync::{Condvar, Mutex};

#[cfg(not(any(debug_assertions, test)))]
pub(crate) type Guard<'a, T> = std::sync::MutexGuard<'a, T>;

#[cfg(any(debug_assertions, test))]
mod debug {
    use std::cell::RefCell;
    use std::collections::{HashMap, HashSet};
    use std::ops::{Deref, DerefMut};
    use std::sync::{Condvar, Mutex, MutexGuard, OnceLock};

    type Class = &'static str;
    type Graph = HashMap<Class, HashSet<Class>>;
    thread_local! { static HELD: RefCell<Vec<(usize, Class)>> = const { RefCell::new(Vec::new()) }; }
    static GRAPH: OnceLock<Mutex<Graph>> = OnceLock::new();

    fn reaches(graph: &Graph, from: Class, target: Class, seen: &mut HashSet<Class>) -> bool {
        from == target || (seen.insert(from) && graph.get(from).is_some_and(|next| next.iter().any(|class| reaches(graph, class, target, seen))))
    }

    fn before(instance: usize, class: Class) {
        HELD.with(|held| {
            let held = held.borrow();
            assert!(!held.iter().any(|(id, _)| *id == instance), "lock reacquisition: {class}");
            let mut graph = GRAPH.get_or_init(|| Mutex::new(Graph::new())).lock().unwrap_or_else(|e| e.into_inner());
            for (_, previous) in held.iter().filter(|(_, previous)| *previous != class) {
                assert!(!reaches(&graph, class, previous, &mut HashSet::new()), "lock order cycle: {previous} -> {class} and {class} -> {previous}");
            }
            for (_, previous) in held.iter().filter(|(_, previous)| *previous != class) {
                graph.entry(previous).or_default().insert(class);
            }
        });
    }

    fn enter(instance: usize, class: Class) { HELD.with(|held| held.borrow_mut().push((instance, class))); }
    fn leave(instance: usize) {
        HELD.with(|held| {
            let mut held = held.borrow_mut();
            if let Some(position) = held.iter().rposition(|(id, _)| *id == instance) {
                held.remove(position);
            }
        });
    }

    pub(crate) fn assert_no_library_lock(boundary: &str) {
        HELD.with(|held| assert!(held.borrow().is_empty(), "{boundary} under library locks: {:?}", *held.borrow()));
    }

    #[cfg(test)]
    pub(crate) fn held_classes() -> Vec<&'static str> { HELD.with(|held| held.borrow().iter().map(|(_, class)| *class).collect()) }

    struct Held { instance: usize, class: Class }
    impl Held {
        fn enter(instance: usize, class: Class) -> Self {
            enter(instance, class);
            Self { instance, class }
        }
    }
    impl Drop for Held { fn drop(&mut self) { leave(self.instance); } }

    pub(crate) struct Guard<'a, T> { held: Held, guard: MutexGuard<'a, T> }
    impl<T> Deref for Guard<'_, T> { type Target = T; fn deref(&self) -> &T { &self.guard } }
    impl<T> DerefMut for Guard<'_, T> { fn deref_mut(&mut self) -> &mut T { &mut self.guard } }

    pub(crate) fn lock<T>(mutex: &Mutex<T>) -> Guard<'_, T> {
        lock_as(mutex, std::any::type_name::<T>())
    }

    pub(crate) fn lock_as<T>(mutex: &Mutex<T>, class: Class) -> Guard<'_, T> {
        let instance = std::ptr::from_ref(mutex) as usize;
        before(instance, class);
        let guard = super::acquire(mutex);
        Guard { held: Held::enter(instance, class), guard }
    }

    pub(crate) fn wait<'a, T>(condition: &Condvar, guard: Guard<'a, T>) -> Guard<'a, T> {
        let Guard { held, guard: raw } = guard;
        let (instance, class) = (held.instance, held.class);
        drop(held);
        before(instance, class);
        let raw = crate::blocking::blocking_unit(|| condition.wait(raw).unwrap_or_else(|e| e.into_inner()));
        Guard { held: Held::enter(instance, class), guard: raw }
    }

}

#[cfg(any(debug_assertions, test))]
pub(crate) use debug::{assert_no_library_lock, lock, wait};
#[cfg(test)]
pub(crate) use debug::held_classes;

#[inline(always)]
pub(crate) fn effect<R>(run: impl FnOnce() -> R) -> R {
    assert_no_library_lock("effect");
    run()
}

#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> Guard<'_, T> { lock_as(mutex, std::any::type_name::<T>()) }
#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn lock_as<'a, T>(mutex: &'a Mutex<T>, _: &'static str) -> Guard<'a, T> { acquire(mutex) }
#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn wait<'a, T>(condition: &Condvar, guard: Guard<'a, T>) -> Guard<'a, T> { crate::blocking::blocking_unit(|| condition.wait(guard).unwrap_or_else(|e| e.into_inner())) }
#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn assert_no_library_lock(_: &str) {}
#[cfg(test)]
#[path = "lock_tests.rs"]
mod tests;
