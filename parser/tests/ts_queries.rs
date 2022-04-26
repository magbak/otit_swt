use otit_dsl_parser::parser::ts_query;

#[test]
fn test_basic_multiline_query() {
    let q = r#"
    ABC-[valve]"HLV"."Mvm"."stVal"
    [valve]."PosPct"."mag"
    from 2021-12-01T00:00:01-01:00
    to 2021-12-02T00:00:01-01:00
    aggregate mean 10min
"#;
    let (s, r) = ts_query(q).expect("No problemo");
    println!("{:?}", r);
}