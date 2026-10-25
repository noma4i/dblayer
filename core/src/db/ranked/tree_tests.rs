use std::collections::BTreeMap;

use super::tree::Tree;

struct Dice(u64);

impl Dice {
    fn next(&mut self, below: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % below
    }
}

fn agrees(tree: &Tree<u64, u64>, model: &BTreeMap<u64, u64>) {
    assert_eq!(tree.len(), model.len());
    assert_eq!(tree.values(), model.values().copied().collect::<Vec<_>>());
    let values: Vec<u64> = model.values().copied().collect();
    for (start, end) in [(0, 0), (0, 3), (2, 9), (5, 5), (model.len().saturating_sub(4), model.len()), (1, model.len() + 5), (model.len() + 1, model.len() + 3)] {
        assert_eq!(tree.slice(start, end), values[start.min(values.len())..end.min(values.len()).max(start.min(values.len()))].to_vec(), "slice {start}..{end}");
    }
    for (rank, (key, value)) in model.iter().enumerate() {
        assert_eq!(tree.get(key), Some(value));
        assert_eq!(tree.rank(key), rank);
    }
}

#[test]
fn random_edits_agree_with_a_sorted_map_at_every_step() {
    let mut dice = Dice(7);
    let mut tree = Tree::<u64, u64>::default();
    let mut model = BTreeMap::new();
    let mut shelf: Vec<(Tree<u64, u64>, BTreeMap<u64, u64>)> = Vec::new();
    for step in 0..3000 {
        let key = dice.next(400);
        if dice.next(3) == 0 {
            tree = tree.without(&key);
            model.remove(&key);
        } else {
            tree = tree.with(key, step);
            model.insert(key, step);
        }
        if step % 25 == 0 {
            agrees(&tree, &model);
            shelf.push((tree.clone(), model.clone()));
        }
    }
    for (old_tree, old_model) in &shelf {
        agrees(old_tree, old_model);
    }
}

#[test]
fn rank_of_an_absent_key_counts_the_smaller_keys() {
    let tree = Tree::from_sorted(&[(10u64, 0u64), (20, 0), (30, 0)]);
    assert_eq!([5u64, 10, 15, 30, 99].map(|key| tree.rank(&key)), [0, 0, 1, 2, 3]);
}

thread_local! {
    static COMPARISONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Clone, PartialEq, Eq)]
struct Counted(u64);

impl PartialOrd for Counted {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Counted {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        COMPARISONS.with(|count| count.set(count.get() + 1));
        self.0.cmp(&other.0)
    }
}

fn deepest_lookup(tree: &Tree<Counted, u64>, keys: impl Iterator<Item = u64>) -> usize {
    keys.map(|key| {
        COMPARISONS.with(|count| count.set(0));
        assert_eq!(tree.get(&Counted(key)), Some(&key));
        COMPARISONS.with(|count| count.get())
    })
    .max()
    .unwrap()
}

#[test]
fn a_tree_stays_balanced_when_built_from_sorted_items_and_under_ascending_inserts_and_removals() {
    let items: Vec<(Counted, u64)> = (0..1000).map(|key| (Counted(key), key)).collect();
    let mut tree = Tree::from_sorted(&items);
    for key in 0..500 {
        tree = tree.without(&Counted(key * 2));
    }
    assert_eq!(tree.len(), 500);
    assert_eq!(tree.values(), (0..500).map(|key| key * 2 + 1).collect::<Vec<_>>());
    assert!(deepest_lookup(&tree, (0..500).map(|key| key * 2 + 1)) <= 18, "removals left a tall tree");

    let mut grown = Tree::<Counted, u64>::default();
    for key in 0..1000 {
        grown = grown.with(Counted(key), key);
        if key % 100 == 99 {
            assert!(deepest_lookup(&grown, 0..=key) <= 18, "ascending inserts built a chain at {} items", key + 1);
        }
    }
}
