use std::os::linux::raw::stat;
use spargebra::Query;
use spargebra::term::TermPattern;
use hybrid::constraints::Constraint;
use hybrid::splitter::parse_sparql_select_query;
use hybrid::static_rewrite::rewrite_static_query;
use hybrid::type_inference::infer_types;

#[test]
fn test_simple_query() {
    let sparql = r#"
    PREFIX qry:<https://github.com/magbak/quarry-rs#>
    SELECT ?var1 ?var2 WHERE {
        ?var1 a ?var2 .
        ?var2 qry:hasTimeseries ?ts .
        ?ts qry:hasDataPoint ?dp .
        ?dp qry:hasValue ?val .
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let tree = infer_types(&parsed);
    let static_rewrite = rewrite_static_query(parsed, &tree).unwrap();

    let expected_str = r#"
    SELECT ?var1 ?var2 WHERE {
     ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
     ?ts <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id .
     ?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts .
      }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}