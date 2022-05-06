use hybrid::splitter::parse_sparql_select_query;
use hybrid::static_query::rewrite_static_query;
use hybrid::type_inference::infer_types;

#[test]
fn test_simple_query() {
    let sparql = r#"SELECT ?var1 ?var2 WHERE {?var1 a ?var2}"#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let tree = infer_types(&parsed);
    let static_rewrite = rewrite_static_query(parsed, &tree);
    println!("{:?}", static_rewrite.unwrap());
}