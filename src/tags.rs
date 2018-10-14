// TODO:
//
// - [x] Parse tag sets, e.g. `"abc" = "ced", "h" = "j"`
// - [ ] Parse tag expressions, e.g. `"abc" == "ced" or "h" == "k"`
// - [ ] Ability to write by string tag set
// - [ ] Ability to read based on tag expression

use pest::Parser;

use std::collections::HashMap;

pub type TagSetID = String;

pub type TagSet = HashMap<String, String>;

pub trait Identifiable {
    fn id(&self) -> TagSetID;
}

impl Identifiable for TagSet {
    fn id(&self) -> TagSetID {
        let mut tags: Vec<String> = self.iter().map(|(k, v)| format!("{}={}", k, v)).collect();

        tags.sort();

        tags.join(",")
    }
}

#[derive(Parser)]
#[grammar = "tags.pest"]
pub struct TagsParser;

pub fn parse_tag_set(input: &str) -> TagSet {
    let rule = TagsParser::parse(Rule::TagSet, input)
        .unwrap()
        .next()
        .unwrap();

    let mut tag_set = TagSet::new();

    for assignment in rule.into_inner() {
        let mut strings = assignment.into_inner();
        let mut next_string = || {
            strings
                .next()
                .unwrap()
                .as_str()
                .trim_matches('"')
                .to_string()
        };

        let key = next_string();
        let value = next_string();

        tag_set.insert(key, value);
    }

    tag_set
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn tags_have_ordered_id() {
        let tag_set_a = parse_tag_set(r#""a" = "A", "b" = "B""#);
        let tag_set_b = parse_tag_set(r#""b" = "B", "a" = "A""#);

        assert_eq!(tag_set_a.id(), "a=A,b=B");
        assert_eq!(tag_set_b.id(), "a=A,b=B");
    }

    #[test]
    fn parse_tag_set_basic() {
        let input = r#""host" = "123", "region" = "us-west""#;
        let result = parse_tag_set(input);

        assert_eq!(result.len(), 2);
        assert_eq!(result.get("host").unwrap(), "123");
        assert_eq!(result.get("region").unwrap(), "us-west");
    }
}
