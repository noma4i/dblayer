use crate::enums::EnumDecl;
use crate::decl::VariantDecl;
use crate::naming::lower_first;

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

fn shared(lists: &[Vec<String>], from_end: bool) -> usize {
    let word = |list: &Vec<String>, index: usize| if from_end { list[list.len() - 1 - index].clone() } else { list[index].clone() };
    let shortest = lists.iter().map(Vec::len).min().expect("a union has variants");
    (0..shortest).take_while(|index| lists.iter().all(|list| word(list, *index) == word(&lists[0], *index))).count()
}

/// The case name of each variant: its type name without the words that every variant starts or ends with. At least one word stays. Equal results are refused by `check_cases`.
pub(crate) fn case_names(typenames: &[&str]) -> Vec<String> {
    let lists: Vec<Vec<String>> = typenames.iter().map(|name| words(name)).collect();
    if lists.len() < 2 {
        return lists.iter().map(|list| lower_first(&list.concat())).collect();
    }
    let shortest = lists.iter().map(Vec::len).min().expect("a union has variants");
    let head = shared(&lists, false).min(shortest - 1);
    let tail = shared(&lists, true).min(shortest - 1 - head);
    lists.iter().map(|list| lower_first(&list[head..list.len() - tail].concat())).collect()
}

pub(crate) fn check_cases(union: &str, cases: &[String]) -> Result<(), String> {
    EnumDecl::new(union, cases).check_as("union", "variant").map_err(|message| format!("{message}. Name a variant with `case:`"))
}

pub(crate) fn cases_of(variants: &[VariantDecl]) -> Vec<String> {
    let derived = case_names(&variants.iter().map(|variant| variant.type_name.as_str()).collect::<Vec<_>>());
    variants.iter().zip(derived).map(|(variant, case)| variant.case.clone().unwrap_or(case)).collect()
}
