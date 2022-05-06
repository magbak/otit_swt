use spargebra::algebra::Expression::Variable;
use spargebra::algebra::GraphPattern;
use spargebra::algebra::GraphPattern::{Bgp, Project};
use spargebra::Query;
use spargebra::Query::Select;
use spargebra::term::{NamedNode, NamedNodePattern, Term, TermPattern, TriplePattern};
use hybrid::splitter::parse_sparql_select_query;
use hybrid::static_query::rewrite_static_query;
use hybrid::type_inference::infer_types;

#[test]
fn test_simple_query() {
    let sparql = r#"
    PREFIX qry:<https://github.com/magbak/quarry-rs#>
    SELECT ?var1 ?var2 WHERE {
        ?var1 a ?var2 .
        ?var2 qry:hasTimeseries ?ts .
        ?ts qry:hasDataPoint ?dp .
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let tree = infer_types(&parsed);
    let static_rewrite = rewrite_static_query(parsed, &tree).unwrap();

    let expected_str = r#"SELECT ?var1 ?var2 WHERE { ?ts <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id .?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts .?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 . }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
    //println!("{}", static_rewrite.to_string());
}