use chrono::{DateTime};
use otit_dsl_parser::ast::ElementConstraint::{Name};
use otit_dsl_parser::ast::{
    Aggregation, BooleanOperator, ConditionedPath, Connective, ConnectiveType, ElementConstraint,
    Glue, GraphPattern, Literal, Path, PathElement, PathElementOrConnective, PathOrLiteral,
    TsQuery,
};
use otit_dsl_parser::parser::ts_query;
use std::str::FromStr;
use std::time::Duration;

#[test]
fn test_basic_api() {
    let q = r#"
    ABC-[valve]"HLV"."Mvm"."stVal"
    [valve]."PosPct"."mag"
    from 2021-12-01T00:00:01+01:00
    to 2021-12-02T00:00:01+01:00
    aggregate mean 10min
"#;
}