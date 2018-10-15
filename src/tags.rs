// TODO:
//
// - [x] Parse tag sets, e.g. `"abc" = "ced", "h" = "j"`
// - [x] Parse tag expressions, e.g. `"abc" == "ced" or "h" == "k"`
// - [ ] Parse tag expressions with parens
// - [ ] Ability to write by string tag set
// - [ ] Ability to read based on tag expression

#![allow(dead_code)] // delete me

use pest::Parser;

use std::collections::HashMap;

#[derive(PartialEq, Debug)]
enum EqualityOp {
    Equals,
    NotEquals,
}

#[derive(PartialEq, Debug)]
struct Equality {
    op: EqualityOp,
    lhs: String,
    rhs: String,
}

#[derive(PartialEq, Debug)]
enum LogicalOp {
    And,
    Or,
}

#[derive(PartialEq, Debug)]
enum Logical {
    Just(Equality),
    Also(Equality, LogicalOp, Box<Logical>),
}

impl Logical {
    fn parse(input: &str) -> Logical {
        let mut rules = TagsParser::parse(Rule::Logical, input)
            .unwrap() // fixme
            .next()
            .unwrap()
            .into_inner();

        let mut equality_rule = rules.next().unwrap().into_inner();

        let lhs = equality_rule
            .next()
            .unwrap()
            .as_str()
            .trim_matches('"')
            .to_string();

        let op = match equality_rule.next().unwrap().as_rule() {
            Rule::Equals => EqualityOp::Equals,
            Rule::NotEquals => EqualityOp::NotEquals,
            _ => unreachable!(),
        };

        let rhs = equality_rule
            .next()
            .unwrap()
            .as_str()
            .trim_matches('"')
            .to_string();

        let equality = Equality { op, lhs, rhs };

        let maybe_op = rules.next();

        if maybe_op.is_none() {
            return Logical::Just(equality);
        }

        let op = match maybe_op.unwrap().as_rule() {
            Rule::And => LogicalOp::And,
            Rule::Or => LogicalOp::Or,
            _ => unreachable!(),
        };

        let next = rules.next().unwrap().as_str();

        return Logical::Also(equality, op, Box::new(Logical::parse(next)));
    }
}

#[derive(Parser)]
#[grammar = "tags.pest"]
pub struct TagsParser;

pub struct TagSet {
    tags: HashMap<String, String>,
}

impl TagSet {
    pub fn id(&self) -> String {
        let mut tags: Vec<String> = self
            .tags
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();

        tags.sort();

        tags.join(",")
    }

    pub fn parse(input: &str) -> TagSet {
        let rule = TagsParser::parse(Rule::TagSet, input)
            .unwrap()
            .next()
            .unwrap();

        let mut tags = HashMap::new();

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

            tags.insert(key, value);
        }

        TagSet { tags }
    }

    pub fn len(&self) -> usize {
        self.tags.len()
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.tags.get(key)
    }

    fn matches_logical(&self, logical: Logical) -> bool {
        match logical {
            Logical::Just(eq) => self.matches_eq(eq),
            Logical::Also(eq, LogicalOp::And, tail) => {
                self.matches_eq(eq) && self.matches_logical(*tail)
            }
            Logical::Also(eq, LogicalOp::Or, tail) => {
                self.matches_eq(eq) || self.matches_logical(*tail)
            }
        }
    }

    fn matches_eq(&self, equality: Equality) -> bool {
        let is_equal = self.get(&equality.lhs) == self.get(&equality.rhs);

        match equality.op {
            EqualityOp::Equals => is_equal,
            EqualityOp::NotEquals => !is_equal,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn tags_have_ordered_id() {
        let tag_set_a = TagSet::parse(r#""a" = "A", "b" = "B""#);
        let tag_set_b = TagSet::parse(r#""b" = "B", "a" = "A""#);

        assert_eq!(tag_set_a.id(), "a=A,b=B");
        assert_eq!(tag_set_b.id(), "a=A,b=B");
    }

    #[test]
    fn parse_tag_set_basic() {
        let input = r#""host" = "123", "region" = "us-west""#;
        let result = TagSet::parse(input);

        assert_eq!(result.len(), 2);
        assert_eq!(result.get("host").unwrap(), "123");
        assert_eq!(result.get("region").unwrap(), "us-west");
    }

    #[test]
    fn parse_logical_just_equals() {
        let input = r#""host" == "123""#;

        let actual = Logical::parse(input);
        let expected = Logical::Just(Equality {
            op: EqualityOp::Equals,
            lhs: "host".to_string(),
            rhs: "123".to_string(),
        });

        assert_eq!(actual, expected)
    }

    #[test]
    fn parse_logical_just_not_equals() {
        let input = r#""host" != "123""#;

        let actual = Logical::parse(input);
        let expected = Logical::Just(Equality {
            op: EqualityOp::NotEquals,
            lhs: "host".to_string(),
            rhs: "123".to_string(),
        });

        assert_eq!(actual, expected)
    }

    #[test]
    fn parse_logical_and_also() {
        let input = r#""host" == "123" and "region" == "us-west""#;

        let actual = Logical::parse(input);
        let expected = Logical::Also(
            Equality {
                op: EqualityOp::Equals,
                lhs: "host".to_string(),
                rhs: "123".to_string(),
            },
            LogicalOp::And,
            Box::new(Logical::Just(Equality {
                op: EqualityOp::Equals,
                lhs: "region".to_string(),
                rhs: "us-west".to_string(),
            })),
        );

        assert_eq!(actual, expected)
    }

    #[test]
    fn parse_logical_or_also() {
        let input = r#""host" == "123" or "region" == "us-west""#;

        let actual = Logical::parse(input);
        let expected = Logical::Also(
            Equality {
                op: EqualityOp::Equals,
                lhs: "host".to_string(),
                rhs: "123".to_string(),
            },
            LogicalOp::Or,
            Box::new(Logical::Just(Equality {
                op: EqualityOp::Equals,
                lhs: "region".to_string(),
                rhs: "us-west".to_string(),
            })),
        );

        assert_eq!(actual, expected)
    }
}
