use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::db::ranked::Ranked;
use crate::identity::RowId;

use super::between::{longest_increasing, Diff};

struct Mover {
    was: usize,
    at: usize,
    id: RowId,
}

impl Diff {
    pub(crate) fn stepping(old: &Ranked, new: &Ranked, ids: &HashSet<String>, replaced: &[(RowId, RowId)]) -> Self {
        let has = |list: &Ranked, id: &RowId| list.get(id.as_str()).is_some();
        let aliases: Vec<&(RowId, RowId)> = replaced.iter().filter(|(from, to)| has(old, from) && has(new, to) && !has(new, from) && !has(old, to)).collect();
        let origin: HashMap<&str, &RowId> = aliases.iter().map(|(from, to)| (to.as_str(), from)).collect();
        let retired: HashSet<&str> = aliases.iter().map(|(from, _)| from.as_str()).collect();
        let renamed = replaced.iter().filter(|(from, to)| has(new, to) && !has(old, to) && !has(new, from)).cloned().collect();
        let mut names: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
        names.extend(replaced.iter().flat_map(|(from, to)| [from.as_str(), to.as_str()]));
        let (mut inserted, mut removed, mut updated, mut swapped, mut movers) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for name in names.into_iter().filter(|name| !retired.contains(name)) {
            let source = origin.get(name).map_or(name, |from| from.as_str());
            match (old.get(source), new.get(name)) {
                (None, None) => {}
                (Some((key, entry)), None) => removed.push((old.position(key), entry.id.clone())),
                (None, Some((key, entry))) => inserted.push((new.position(key), entry.id.clone())),
                (Some((before, was)), Some((after, now))) => {
                    let (from_at, at) = (old.position(before), new.position(after));
                    if origin.contains_key(name) {
                        swapped.push((from_at, at, was.id.clone(), now.id.clone()));
                    } else if was.stamp != now.stamp {
                        updated.push((at, now.id.clone()));
                    }
                    if before != after {
                        movers.push(Mover { was: from_at, at, id: now.id.clone() });
                    }
                }
            }
        }
        let mut gone: Vec<usize> = movers.iter().map(|mover| mover.was).chain(removed.iter().map(|(at, _)| *at)).collect();
        let mut came: Vec<usize> = movers.iter().map(|mover| mover.at).chain(inserted.iter().map(|(at, _)| *at)).collect();
        gone.sort_unstable();
        came.sort_unstable();
        let (mut level, mut shifted): (Vec<Mover>, Vec<Mover>) = movers
            .into_iter()
            .partition(|mover| mover.was - gone.partition_point(|position| *position < mover.was) == mover.at - came.partition_point(|position| *position < mover.at));
        level.sort_by_key(|mover| mover.was);
        let kept = longest_increasing(&level.iter().map(|mover| mover.at).collect::<Vec<_>>());
        shifted.extend(level.into_iter().enumerate().filter(|(index, _)| !kept.contains(index)).map(|(_, mover)| mover));
        shifted.sort_by_key(|mover| mover.at);
        inserted.sort_by_key(|(at, _)| *at);
        removed.sort_by_key(|(at, _)| *at);
        updated.sort_by_key(|(at, _)| *at);
        swapped.sort_by_key(|(was, _, _, _)| *was);
        let mut at: BTreeMap<RowId, usize> = inserted.iter().chain(&updated).map(|(position, id)| (id.clone(), *position)).collect();
        at.extend(shifted.iter().map(|mover| (mover.id.clone(), mover.at)));
        at.extend(swapped.iter().map(|(_, position, _, to)| (to.clone(), *position)));
        let mut was: BTreeMap<RowId, usize> = removed.iter().map(|(position, id)| (id.clone(), *position)).collect();
        was.extend(shifted.iter().map(|mover| (mover.id.clone(), mover.was)));
        was.extend(swapped.iter().map(|(position, _, _, to)| (to.clone(), *position)));
        Diff {
            inserted: inserted.into_iter().map(|(_, id)| id).collect(),
            removed: removed.into_iter().map(|(_, id)| id).collect(),
            updated: updated.into_iter().map(|(_, id)| id).collect(),
            moved: shifted.into_iter().map(|mover| mover.id).collect(),
            replaced: swapped.into_iter().map(|(_, _, from, to)| (from, to)).collect(),
            renamed,
            at,
            was,
        }
    }
}
