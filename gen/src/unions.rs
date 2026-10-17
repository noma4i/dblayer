use crate::enums::EnumDecl;
use crate::parser::{Call, Node, Pair, Value};

fn words(name: &str) -> Vec<String> {
    let characters: Vec<char> = name.chars().collect();
    let mut found: Vec<String> = Vec::new();
    for (at, character) in characters.iter().enumerate() {
        let after_lower = at > 0 && characters[at - 1].is_lowercase();
        let ends_acronym = at > 0 && characters[at - 1].is_uppercase() && characters.get(at + 1).is_some_and(|next| next.is_lowercase());
        if found.is_empty() || (character.is_uppercase() && (after_lower || ends_acronym)) {
            found.push(String::new());
        }
        found.last_mut().expect("a word was pushed").push(*character);
    }
    found
}

fn lower_first(text: &str) -> String {
    let mut characters = text.chars();
    characters.next().map(|first| first.to_lowercase().collect::<String>() + characters.as_str()).unwrap_or_default()
}

fn shared(lists: &[Vec<String>], from_end: bool) -> usize {
    let at = |list: &Vec<String>, index: usize| if from_end { list[list.len() - 1 - index].clone() } else { list[index].clone() };
    let limit = lists.iter().map(Vec::len).min().unwrap_or(0);
    (0..limit).take_while(|index| lists.iter().all(|list| at(list, *index) == at(&lists[0], *index))).count()
}

/// The case name of each variant: its type name without the words that every variant starts or ends with, or the whole name when stripping would leave an empty or repeated name.
pub(crate) fn case_names(typenames: &[&str]) -> Vec<String> {
    let lists: Vec<Vec<String>> = typenames.iter().map(|name| words(name)).collect();
    let whole: Vec<String> = lists.iter().map(|list| lower_first(&list.concat())).collect();
    if lists.len() < 2 {
        return whole;
    }
    let shortest = lists.iter().map(Vec::len).min().unwrap_or(0);
    let head = shared(&lists, false).min(shortest - 1);
    let tail = shared(&lists, true).min(shortest - 1 - head);
    let trimmed: Vec<String> = lists.iter().map(|list| lower_first(&list[head..list.len() - tail].concat())).collect();
    let distinct = trimmed.iter().enumerate().all(|(at, name)| !trimmed[..at].contains(name));
    if distinct { trimmed } else { whole }
}

pub(crate) fn check_cases(union: &str, cases: &[String]) -> Result<(), String> {
    EnumDecl::new(union, cases).check_as("union", "variant")
}

fn typename_of(variant: &Call) -> Option<&str> {
    match variant.args.first().map(|node| &node.item) {
        Some(Value::Const(name)) => Some(name),
        _ => None,
    }
}

pub(crate) fn cases_of(variants: &[Call]) -> Vec<String> {
    let named: Vec<&Call> = variants.iter().filter(|variant| typename_of(variant).is_some()).collect();
    let derived = case_names(&named.iter().filter_map(|variant| typename_of(variant)).collect::<Vec<_>>());
    named
        .iter()
        .zip(derived)
        .map(|(variant, case)| match variant.options.iter().find(|pair| pair.key == "case") {
            Some(Pair { value: Node { item: Value::Symbol(over), .. }, .. }) => over.clone(),
            _ => case,
        })
        .collect()
}
