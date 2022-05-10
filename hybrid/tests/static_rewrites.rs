use hybrid::splitter::parse_sparql_select_query;
use hybrid::static_rewrite::StaticQueryRewriter;
use hybrid::type_inference::infer_types;
use log::debug;
use spargebra::Query;

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
    let mut rewriter = StaticQueryRewriter::new();
    let static_rewrite = rewriter.rewrite_static_query(parsed, &tree).unwrap();

    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_external_id_0 WHERE {
     ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
     ?ts <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_0 .
     ?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts .
      }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_filtered_query() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/quarry-rs#>
    SELECT ?var1 ?var2 WHERE {
        ?var1 a ?var2 .
        ?var2 qry:hasTimeseries ?ts .
        ?ts qry:hasDataPoint ?dp .
        ?dp qry:hasValue ?val .
        ?dp qry:hasTimestamp ?t .
        FILTER(?val > 0.5 && ?t >= "2016-01-01"^^xsd:dateTime)
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let tree = infer_types(&parsed);
    let mut rewriter = StaticQueryRewriter::new();
    let static_rewrite = rewriter.rewrite_static_query(parsed, &tree).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_external_id_0 WHERE {
     ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
     ?ts <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_0 .
     ?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts .
      }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_complex_expression_filter() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/quarry-rs#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 WHERE {
        ?var1 a ?var2 .
        ?var2 ex:hasPropertyValue ?pv .
        ?var2 qry:hasTimeseries ?ts .
        ?ts qry:hasDataPoint ?dp .
        ?dp qry:hasValue ?val .
        ?dp qry:hasTimestamp ?t .
        FILTER(?val > 0.5 && ?t >= "2016-01-01"^^xsd:dateTime && ?pv)
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    debug!("Hello test");
    let tree = infer_types(&parsed);
    let mut rewriter = StaticQueryRewriter::new();
    let static_rewrite = rewriter.rewrite_static_query(parsed, &tree).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_external_id_0 WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?var2 <https://example.com/hasPropertyValue> ?pv .
    ?ts <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_0 .
    ?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts .
    FILTER(?pv) }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_complex_nested_expression_filter() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/quarry-rs#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 WHERE {
        ?var1 a ?var2 .
        ?var2 ex:hasPropertyValue ?pv .
        ?var2 qry:hasTimeseries ?ts .
        ?ts qry:hasDataPoint ?dp .
        ?dp qry:hasValue ?val .
        ?dp qry:hasTimestamp ?t .
        FILTER(?val <= 0.5 || !(?pv))
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let tree = infer_types(&parsed);
    let mut rewriter = StaticQueryRewriter::new();
    let static_rewrite = rewriter.rewrite_static_query(parsed, &tree).unwrap();
    println!("{}", static_rewrite);
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_external_id_0 WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?var2 <https://example.com/hasPropertyValue> ?pv .
    ?ts <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_0 .
    ?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts .
     }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_option_expression_filter_projection() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/quarry-rs#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 ?pv ?t ?val WHERE {
        ?var1 a ?var2 .
        OPTIONAL {
            ?var2 ex:hasPropertyValue ?pv .
            ?var2 qry:hasTimeseries ?ts .
            ?ts qry:hasDataPoint ?dp .
            ?dp qry:hasValue ?val .
            ?dp qry:hasTimestamp ?t .
            FILTER(?val <= 0.5 && !(?pv))
        }
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let tree = infer_types(&parsed);
    let mut rewriter = StaticQueryRewriter::new();
    let static_rewrite = rewriter.rewrite_static_query(parsed, &tree).unwrap();
    println!("{}", static_rewrite);
    let expected_str = r#"
    SELECT ?var1 ?var2 ?pv ?ts_external_id_0 WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    OPTIONAL {
    ?var2 <https://example.com/hasPropertyValue> ?pv .
    ?ts <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_0 .
    ?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts .
    FILTER(!(?pv))
    }
     }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_union_expression() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/quarry-rs#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 ?pv WHERE {
        ?var1 a ?var2 .
        OPTIONAL {
            ?var2 ex:hasPropertyValue ?pv .
            {
            ?var2 qry:hasTimeseries ?ts .
            ?ts qry:hasDataPoint ?dp .
            ?dp qry:hasValue ?val .
            ?dp qry:hasTimestamp ?t .
            FILTER(?val <= 0.5 && !(?pv))
            } UNION {
            ?var2 qry:hasTimeseries ?ts .
            ?ts qry:hasDataPoint ?dp .
            ?dp qry:hasValue ?val .
            ?dp qry:hasTimestamp ?t .
            FILTER(?val > 100.0 && ?pv)
            }
            }
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let tree = infer_types(&parsed);
    let mut rewriter = StaticQueryRewriter::new();
    let static_rewrite = rewriter.rewrite_static_query(parsed, &tree).unwrap();
    println!("{}", static_rewrite);
    let expected_str = r#"
    SELECT ?var1 ?var2 ?pv ?ts_external_id_0 ?ts_external_id_1 WHERE {
        ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
        OPTIONAL {
        ?var2 <https://example.com/hasPropertyValue> ?pv .
            {
              ?ts <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_0 .
              ?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts .
              FILTER(!?pv)
            }
            UNION {
              ?ts <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_1 .
              ?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts .
              FILTER(?pv)
            }
        }
    }
    "#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}
