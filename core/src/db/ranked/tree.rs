use std::borrow::Borrow;
use std::cmp::Ordering;
use std::sync::Arc;

type Link<K, V> = Option<Arc<Node<K, V>>>;

struct Node<K, V> {
    key: K,
    value: V,
    left: Link<K, V>,
    right: Link<K, V>,
    size: usize,
    height: u8,
}

pub(crate) struct Tree<K, V> {
    root: Link<K, V>,
}

impl<K, V> Clone for Tree<K, V> {
    fn clone(&self) -> Self {
        Self { root: self.root.clone() }
    }
}

impl<K, V> Default for Tree<K, V> {
    fn default() -> Self {
        Self { root: None }
    }
}

fn size<K, V>(link: &Link<K, V>) -> usize {
    link.as_ref().map_or(0, |node| node.size)
}

fn height<K, V>(link: &Link<K, V>) -> u8 {
    link.as_ref().map_or(0, |node| node.height)
}

fn make<K, V>(key: K, value: V, left: Link<K, V>, right: Link<K, V>) -> Arc<Node<K, V>> {
    let (size, height) = (size(&left) + size(&right) + 1, height(&left).max(height(&right)) + 1);
    Arc::new(Node { key, value, left, right, size, height })
}

fn balanced<K: Clone, V: Clone>(key: K, value: V, left: Link<K, V>, right: Link<K, V>) -> Arc<Node<K, V>> {
    let (tall_left, tall_right) = (height(&left), height(&right));
    if tall_left > tall_right + 1 {
        if let Some(heavy) = &left {
            if height(&heavy.left) >= height(&heavy.right) {
                return make(heavy.key.clone(), heavy.value.clone(), heavy.left.clone(), Some(make(key, value, heavy.right.clone(), right)));
            }
            if let Some(inner) = &heavy.right {
                let lower = make(heavy.key.clone(), heavy.value.clone(), heavy.left.clone(), inner.left.clone());
                return make(inner.key.clone(), inner.value.clone(), Some(lower), Some(make(key, value, inner.right.clone(), right)));
            }
        }
    }
    if tall_right > tall_left + 1 {
        if let Some(heavy) = &right {
            if height(&heavy.right) >= height(&heavy.left) {
                return make(heavy.key.clone(), heavy.value.clone(), Some(make(key, value, left, heavy.left.clone())), heavy.right.clone());
            }
            if let Some(inner) = &heavy.left {
                let higher = make(heavy.key.clone(), heavy.value.clone(), inner.right.clone(), heavy.right.clone());
                return make(inner.key.clone(), inner.value.clone(), Some(make(key, value, left, inner.left.clone())), Some(higher));
            }
        }
    }
    make(key, value, left, right)
}

fn insert<K: Ord + Clone, V: Clone>(link: &Link<K, V>, key: K, value: V) -> Arc<Node<K, V>> {
    let Some(node) = link else { return make(key, value, None, None) };
    match key.cmp(&node.key) {
        Ordering::Less => balanced(node.key.clone(), node.value.clone(), Some(insert(&node.left, key, value)), node.right.clone()),
        Ordering::Greater => balanced(node.key.clone(), node.value.clone(), node.left.clone(), Some(insert(&node.right, key, value))),
        Ordering::Equal => make(key, value, node.left.clone(), node.right.clone()),
    }
}

fn pop_first<K: Clone, V: Clone>(node: &Arc<Node<K, V>>) -> (K, V, Link<K, V>) {
    let Some(left) = &node.left else { return (node.key.clone(), node.value.clone(), node.right.clone()) };
    let (key, value, rest) = pop_first(left);
    (key, value, Some(balanced(node.key.clone(), node.value.clone(), rest, node.right.clone())))
}

fn remove<K: Ord + Clone + Borrow<Q>, V: Clone, Q: Ord + ?Sized>(link: &Link<K, V>, key: &Q) -> Link<K, V> {
    let node = link.as_ref()?;
    match key.cmp(node.key.borrow()) {
        Ordering::Less => Some(balanced(node.key.clone(), node.value.clone(), remove(&node.left, key), node.right.clone())),
        Ordering::Greater => Some(balanced(node.key.clone(), node.value.clone(), node.left.clone(), remove(&node.right, key))),
        Ordering::Equal => match (&node.left, &node.right) {
            (None, right) => right.clone(),
            (left, None) => left.clone(),
            (left, Some(right)) => {
                let (next_key, next_value, rest) = pop_first(right);
                Some(balanced(next_key, next_value, left.clone(), rest))
            }
        },
    }
}

fn build<K: Clone, V: Clone>(items: &[(K, V)]) -> Link<K, V> {
    if items.is_empty() {
        return None;
    }
    let middle = items.len() / 2;
    let (key, value) = items[middle].clone();
    Some(make(key, value, build(&items[..middle]), build(&items[middle + 1..])))
}

impl<K: Ord + Clone, V: Clone> Tree<K, V> {
    pub(crate) fn from_sorted(items: &[(K, V)]) -> Self {
        Self { root: build(items) }
    }

    pub(crate) fn len(&self) -> usize {
        size(&self.root)
    }

    pub(crate) fn get<Q: Ord + ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        let mut link = &self.root;
        while let Some(node) = link {
            match key.cmp(node.key.borrow()) {
                Ordering::Less => link = &node.left,
                Ordering::Greater => link = &node.right,
                Ordering::Equal => return Some(&node.value),
            }
        }
        None
    }

    pub(crate) fn rank<Q: Ord + ?Sized>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
    {
        let (mut link, mut below) = (&self.root, 0);
        while let Some(node) = link {
            match key.cmp(node.key.borrow()) {
                Ordering::Less => link = &node.left,
                Ordering::Equal => return below + size(&node.left),
                Ordering::Greater => {
                    below += size(&node.left) + 1;
                    link = &node.right;
                }
            }
        }
        below
    }

    pub(crate) fn with(&self, key: K, value: V) -> Self {
        Self { root: Some(insert(&self.root, key, value)) }
    }

    pub(crate) fn without<Q: Ord + ?Sized>(&self, key: &Q) -> Self
    where
        K: Borrow<Q>,
    {
        Self { root: remove(&self.root, key) }
    }

    pub(crate) fn values(&self) -> Vec<V> {
        let mut found = Vec::with_capacity(self.len());
        let mut stack: Vec<&Arc<Node<K, V>>> = Vec::new();
        let mut link = &self.root;
        loop {
            while let Some(node) = link {
                stack.push(node);
                link = &node.left;
            }
            let Some(node) = stack.pop() else { return found };
            found.push(node.value.clone());
            link = &node.right;
        }
    }
}
