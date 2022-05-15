use hybrid::preprocessing::Preprocessor;
use hybrid::splitter::parse_sparql_select_query;
use hybrid::static_rewrite::StaticQueryRewriter;
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
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let static_rewrite = rewriter
        .rewrite_static_query(preprocessed_query)
        .unwrap();

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
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let static_rewrite = rewriter
        .rewrite_static_query(preprocessed_query)
        .unwrap();
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
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let static_rewrite = rewriter
        .rewrite_static_query(preprocessed_query)
        .unwrap();
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
fn test_complex_expression_filter_projection() {
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
        FILTER(?val > ?pv || ?t >= "2016-01-01"^^xsd:dateTime)
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let static_rewrite = rewriter
        .rewrite_static_query(preprocessed_query)
        .unwrap();
    println!("{}", static_rewrite);
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_external_id_0 ?pv WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?var2 <https://example.com/hasPropertyValue> ?pv .
    ?ts <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_0 .
    ?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts . }
    "#;
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
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let static_rewrite = rewriter
        .rewrite_static_query(preprocessed_query)
        .unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_external_id_0 ?pv WHERE {
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
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let static_rewrite = rewriter
        .rewrite_static_query(preprocessed_query)
        .unwrap();
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
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let static_rewrite = rewriter
        .rewrite_static_query(preprocessed_query)
        .unwrap();
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

#[test]
fn test_bind_expression() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/quarry-rs#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 ?val3 WHERE {
        ?var1 a ?var2 .
        ?var1 qry:hasTimeseries ?ts1 .
        ?ts1 qry:hasDataPoint ?dp1 .
        ?dp1 qry:hasValue ?val1 .
        ?dp1 qry:hasTimestamp ?t .
        ?var2 qry:hasTimeseries ?ts2 .
        ?ts2 qry:hasDataPoint ?dp2 .
        ?dp2 qry:hasValue ?val2 .
        ?dp2 qry:hasTimestamp ?t .
        BIND((?val1 + ?val2) as ?val3)
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let static_rewrite = rewriter
        .rewrite_static_query(preprocessed_query)
        .unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?val3 ?ts_external_id_0 ?ts_external_id_1 WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?ts1 <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_0 .
    ?var1 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts1 .
    ?ts2 <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_1 .
    ?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?ts2 . }
    "#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_property_path_expression() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/quarry-rs#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 ?val3 WHERE {
        ?var1 a ?var2 .
        ?var1 qry:hasTimeseries / qry:hasDataPoint ?dp1 .
        ?dp1 qry:hasValue ?val1 .
        ?dp1 qry:hasTimestamp ?t .
        ?var2 qry:hasTimeseries / qry:hasDataPoint ?dp2 .
        ?dp2 qry:hasValue ?val2 .
        ?dp2 qry:hasTimestamp ?t .
        BIND((?val1 + ?val2) as ?val3)
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let static_rewrite = rewriter
        .rewrite_static_query(preprocessed_query)
        .unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?val3 ?ts_external_id_0 ?ts_external_id_1 WHERE {
     ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
     ?blank_replacement_0 <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_0 .
     ?var1 <https://github.com/magbak/quarry-rs#hasTimeseries> ?blank_replacement_0 .
     ?blank_replacement_1 <https://github.com/magbak/quarry-rs#hasExternalId> ?ts_external_id_1 .
     ?var2 <https://github.com/magbak/quarry-rs#hasTimeseries> ?blank_replacement_1 . }
    "#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}
