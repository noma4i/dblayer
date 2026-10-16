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

#[test]
fn a_tree_built_from_sorted_items_stays_balanced_under_edits() {
    let items: Vec<(u64, u64)> = (0..1000).map(|key| (key, key)).collect();
    let mut tree = Tree::from_sorted(&items);
    for key in 0..500 {
        tree = tree.without(&(key * 2));
    }
    assert_eq!(tree.len(), 500);
    assert_eq!(tree.values(), (0..500).map(|key| key * 2 + 1).collect::<Vec<_>>());
}
