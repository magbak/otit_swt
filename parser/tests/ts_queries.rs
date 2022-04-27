use chrono::{DateTime};
use otit_dsl_parser::ast::ElementConstraint::{Name};
use otit_dsl_parser::ast::{Aggregation, BooleanOperator, ConditionedPath, Connective, ConnectiveType, ElementConstraint, Glue, GraphPattern, Group, Literal, Path, PathElement, PathElementOrConnective, PathOrLiteral, TsQuery};
use otit_dsl_parser::parser::ts_query;
use std::str::FromStr;
use std::time::Duration;

#[test]
fn test_basic_multiline_query() {
    let q = r#"
    ABC-[valve]"HLV"."Mvm"."stVal"
    [valve]."PosPct"."mag"
    group valve
    from 2021-12-01T00:00:01+01:00
    to 2021-12-02T00:00:01+01:00
    aggregate mean 10min
"#;
    let (_, actual) = ts_query(q).expect("No problemo");
    let expected = TsQuery::new(
        GraphPattern::new(vec![
            ConditionedPath::from_path(
                Path::new(vec![
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(ElementConstraint::TypeName("ABC".to_string())),
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Dash, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        Some(Glue::new("valve")),
                        Some(Name("HLV".to_string())),
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Period, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(Name("Mvm".to_string())),
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Period, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(Name("stVal".to_string())),
                    )),
                ])
            ),
            ConditionedPath::from_path(
                Path::new(vec![
                    PathElementOrConnective::PathElement(PathElement::new(
                        Some(Glue::new("valve")),
                        None,
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Period, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(Name("PosPct".to_string())),
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Period, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(Name("mag".to_string())),
                    )),
                ]),
            ),
        ]),
        Group::new(vec!["valve"]),
        DateTime::from_str("2021-11-30T23:00:01Z").expect("ParseOk"),
        DateTime::from_str("2021-12-01T23:00:01Z").expect("ParseOk"),
        Aggregation::new("mean", Duration::from_secs(600)),
    );
    assert_eq!(expected, actual);
}

#[test]
fn test_conditioned_multiline_query() {
    let q = r#"
    ABC-[valve]"HLV"."Mvm"."stVal" = true
    [valve]."PosPct"."mag" > 0.7
    group valve
    from 2021-12-01T00:00:01+01:00
    to 2021-12-02T00:00:01+01:00
    aggregate mean 10min
"#;
    let (_, actual) = ts_query(q).expect("No problemo");
    let expected = TsQuery::new(
        GraphPattern::new(vec![
            ConditionedPath::new(
                Path::new(vec![
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(ElementConstraint::TypeName("ABC".to_string())),
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Dash, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        Some(Glue::new("valve")),
                        Some(Name("HLV".to_string())),
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Period, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(Name("Mvm".to_string())),
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Period, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(Name("stVal".to_string())),
                    )),
                ]),
                BooleanOperator::EQ,
                PathOrLiteral::Literal(Literal::Boolean(true)),
            ),
            ConditionedPath::new(
                Path::new(vec![
                    PathElementOrConnective::PathElement(PathElement::new(
                        Some(Glue::new("valve")),
                        None,
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Period, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(Name("PosPct".to_string())),
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Period, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(Name("mag".to_string())),
                    )),
                ]),
                BooleanOperator::GT,
                PathOrLiteral::Literal(Literal::Real(0.7)),
            ),
        ]),
        Group::new(vec!["valve"]),
        DateTime::from_str("2021-11-30T23:00:01Z").expect("ParseOk"),
        DateTime::from_str("2021-12-01T23:00:01Z").expect("ParseOk"),
        Aggregation::new("mean", Duration::from_secs(600)),
    );
    assert_eq!(expected, actual);
}
