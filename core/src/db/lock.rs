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
            let position = held.iter().rposition(|(id, _)| *id == instance).expect("tracked lock missing");
            held.remove(position);
        });
    }

    pub(crate) fn assert_no_library_lock(boundary: &str) {
        HELD.with(|held| assert!(held.borrow().is_empty(), "{boundary} under library locks: {:?}", *held.borrow()));
    }

    pub(crate) fn holds<T>() -> bool {
        HELD.with(|held| held.borrow().iter().any(|(_, class)| *class == std::any::type_name::<T>()))
    }

    #[cfg(test)]
    pub(crate) fn is_empty() -> bool { HELD.with(|held| held.borrow().is_empty()) }
    #[cfg(test)]
    pub(crate) fn held_classes() -> Vec<&'static str> { HELD.with(|held| held.borrow().iter().map(|(_, class)| *class).collect()) }

    pub(crate) struct Guard<'a, T> { guard: Option<MutexGuard<'a, T>>, instance: usize, class: Class }
    impl<T> Deref for Guard<'_, T> { type Target = T; fn deref(&self) -> &T { self.guard.as_deref().expect("tracked guard missing") } }
    impl<T> DerefMut for Guard<'_, T> { fn deref_mut(&mut self) -> &mut T { self.guard.as_deref_mut().expect("tracked guard missing") } }
    impl<T> Drop for Guard<'_, T> { fn drop(&mut self) { if self.guard.is_some() { leave(self.instance); } } }

    pub(crate) fn lock<T>(mutex: &Mutex<T>) -> Guard<'_, T> {
        lock_as(mutex, std::any::type_name::<T>())
    }

    pub(crate) fn lock_as<T>(mutex: &Mutex<T>, class: Class) -> Guard<'_, T> {
        let instance = std::ptr::from_ref(mutex) as usize;
        before(instance, class);
        let guard = mutex.lock().unwrap_or_else(|e| e.into_inner());
        enter(instance, class);
        Guard { guard: Some(guard), instance, class }
    }

    pub(crate) fn try_lock<T>(mutex: &Mutex<T>) -> Option<Guard<'_, T>> {
        let instance = std::ptr::from_ref(mutex) as usize;
        let class = std::any::type_name::<T>();
        let guard = match mutex.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        before(instance, class);
        enter(instance, class);
        Some(Guard { guard: Some(guard), instance, class })
    }

    pub(crate) fn wait<'a, T>(condition: &Condvar, mut guard: Guard<'a, T>) -> Guard<'a, T> {
        let raw = guard.guard.take().expect("tracked guard missing");
        leave(guard.instance);
        before(guard.instance, guard.class);
        let raw = condition.wait(raw).unwrap_or_else(|e| e.into_inner());
        enter(guard.instance, guard.class);
        guard.guard = Some(raw);
        guard
    }

    pub(crate) fn wait_timeout_while<'a, T>(condition: &Condvar, mut guard: Guard<'a, T>, duration: std::time::Duration, mut predicate: impl FnMut(&mut T) -> bool) -> (Guard<'a, T>, std::sync::WaitTimeoutResult) {
        let raw = guard.guard.take().expect("tracked guard missing");
        leave(guard.instance);
        before(guard.instance, guard.class);
        struct PredicateGuard(usize);
        impl Drop for PredicateGuard { fn drop(&mut self) { leave(self.0); } }
        let (raw, result) = condition.wait_timeout_while(raw, duration, |value| {
            enter(guard.instance, guard.class);
            let _entry = PredicateGuard(guard.instance);
            predicate(value)
        }).unwrap_or_else(|e| e.into_inner());
        enter(guard.instance, guard.class);
        guard.guard = Some(raw);
        (guard, result)
    }

    pub(crate) fn wait_timeout<'a, T>(condition: &Condvar, mut guard: Guard<'a, T>, duration: std::time::Duration) -> (Guard<'a, T>, std::sync::WaitTimeoutResult) {
        let raw = guard.guard.take().expect("tracked guard missing");
        leave(guard.instance);
        before(guard.instance, guard.class);
        let (raw, result) = condition.wait_timeout(raw, duration).unwrap_or_else(|e| e.into_inner());
        enter(guard.instance, guard.class);
        guard.guard = Some(raw);
        (guard, result)
    }
}

#[cfg(any(debug_assertions, test))]
pub(crate) use debug::{assert_no_library_lock, holds, lock, lock_as, try_lock, wait, wait_timeout, wait_timeout_while, Guard};
#[cfg(test)]
pub(crate) use debug::{held_classes, is_empty};

#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> Guard<'_, T> { lock_as(mutex, std::any::type_name::<T>()) }
#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn lock_as<'a, T>(mutex: &'a Mutex<T>, _: &'static str) -> Guard<'a, T> { mutex.lock().unwrap_or_else(|e| e.into_inner()) }
#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn try_lock<T>(mutex: &Mutex<T>) -> Option<Guard<'_, T>> {
    match mutex.try_lock() { Ok(guard) => Some(guard), Err(std::sync::TryLockError::Poisoned(e)) => Some(e.into_inner()), Err(std::sync::TryLockError::WouldBlock) => None }
}
#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn wait<'a, T>(condition: &Condvar, guard: Guard<'a, T>) -> Guard<'a, T> { condition.wait(guard).unwrap_or_else(|e| e.into_inner()) }
#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn assert_no_library_lock(_: &str) {}
#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn wait_timeout_while<'a, T>(condition: &Condvar, guard: Guard<'a, T>, duration: std::time::Duration, predicate: impl FnMut(&mut T) -> bool) -> (Guard<'a, T>, std::sync::WaitTimeoutResult) {
    condition.wait_timeout_while(guard, duration, predicate).unwrap_or_else(|e| e.into_inner())
}
#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn wait_timeout<'a, T>(condition: &Condvar, guard: Guard<'a, T>, duration: std::time::Duration) -> (Guard<'a, T>, std::sync::WaitTimeoutResult) {
    condition.wait_timeout(guard, duration).unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::debug::{lock, lock_as};
    use crate::db::dispatch::Effects;
    use std::sync::Mutex;

    fn message(error: Box<dyn std::any::Any + Send>) -> String {
        error.downcast_ref::<String>().cloned().or_else(|| error.downcast_ref::<&str>().map(|text| (*text).to_string())).unwrap_or_default()
    }

    #[test]
    fn debug_lock_graph_rejects_cycle() {
        let first = Mutex::new(());
        let second = Mutex::new(());
        { let _a = lock_as(&first, "test graph first"); let _b = lock_as(&second, "test graph second"); }
        let _b = lock_as(&second, "test graph second");
        let result = std::panic::catch_unwind(|| { let _a = lock_as(&first, "test graph first"); });
        let diagnostic = message(result.expect_err("the inverse acquisition must fail before blocking"));
        assert!(diagnostic.contains("test graph first") && diagnostic.contains("test graph second"));
    }

    #[test]
    fn same_instance_reacquire_is_diagnosed() {
        let mutex = Mutex::new(());
        let _guard = lock_as(&mutex, "test reacquire");
        let result = std::panic::catch_unwind(|| { let _again = lock_as(&mutex, "test reacquire"); });
        let diagnostic = message(result.expect_err("reacquiring the same mutex must fail before blocking"));
        assert!(diagnostic.contains("test reacquire"));
    }

    #[test]
    fn effect_entry_rejects_guard() {
        struct EffectLock;
        let mutex = Mutex::new(EffectLock);
        let _guard = lock(&mutex);
        let result = std::panic::catch_unwind(|| Effects::default().run());
        let diagnostic = message(result.expect_err("effects must reject an active library guard"));
        assert!(diagnostic.contains("EffectLock") && diagnostic.contains("Effects::run"));
    }
}
