use std::sync::OnceLock;

struct Node<T> {
    value: T,
    next: OnceLock<Box<Node<T>>>,
}

pub struct AppendLog<T> {
    head: OnceLock<Box<Node<T>>>,
}

impl<T> Default for AppendLog<T> {
    fn default() -> Self { Self { head: OnceLock::new() } }
}

impl<T> AppendLog<T> {
    pub fn push(&self, value: T) {
        let mut slot = &self.head;
        let mut node = Box::new(Node { value, next: OnceLock::new() });
        loop {
            match slot.set(node) {
                Ok(()) => return,
                Err(back) => node = back,
            }
            let Some(taken) = slot.get() else { continue };
            slot = &taken.next;
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        let mut next = self.head.get();
        std::iter::from_fn(move || {
            let node = next?;
            next = node.next.get();
            Some(&node.value)
        })
    }
}

impl<T> Drop for AppendLog<T> {
    fn drop(&mut self) {
        let mut next = self.head.take();
        while let Some(mut node) = next {
            next = node.next.take();
        }
    }
}
