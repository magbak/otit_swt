use hybrid::preprocessing::Preprocessor;
use hybrid::query_context::{Context, PathEntry, VariableInContext};
use hybrid::rewriting::StaticQueryRewriter;
use hybrid::splitter::parse_sparql_select_query;
use hybrid::timeseries_query::BasicTimeSeriesQuery;
use spargebra::term::Variable;
use spargebra::Query;

#[test]
fn test_simple_query() {
    let sparql = r#"
    PREFIX qry:<https://github.com/magbak/otit_swt#>
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
    let (static_rewrite, _) = rewriter.rewrite_query(preprocessed_query).unwrap();

    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_datatype_0 ?ts_external_id_0 WHERE {
     ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
     ?ts <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
     ?ts <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
     ?var2 <https://github.com/magbak/otit_swt#hasTimeseries> ?ts .
      }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_filtered_query() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/otit_swt#>
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
    let (static_rewrite, _) = rewriter.rewrite_query(preprocessed_query).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_datatype_0 ?ts_external_id_0 WHERE {
     ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
     ?ts <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
     ?ts <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
     ?var2 <https://github.com/magbak/otit_swt#hasTimeseries> ?ts .
      }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_complex_expression_filter() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/otit_swt#>
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
    let (static_rewrite, _) = rewriter.rewrite_query(preprocessed_query).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_datatype_0 ?ts_external_id_0 WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?var2 <https://example.com/hasPropertyValue> ?pv .
    ?ts <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
    ?ts <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
    ?var2 <https://github.com/magbak/otit_swt#hasTimeseries> ?ts .
    FILTER(?pv) }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_complex_expression_filter_projection() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/otit_swt#>
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
    let (static_rewrite, _) = rewriter.rewrite_query(preprocessed_query).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_datatype_0 ?ts_external_id_0 ?pv WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?var2 <https://example.com/hasPropertyValue> ?pv .
    ?ts <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
    ?ts <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
    ?var2 <https://github.com/magbak/otit_swt#hasTimeseries> ?ts . }
    "#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_complex_nested_expression_filter() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/otit_swt#>
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
    let (static_rewrite, _) = rewriter.rewrite_query(preprocessed_query).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_datatype_0 ?ts_external_id_0 ?pv WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?var2 <https://example.com/hasPropertyValue> ?pv .
    ?ts <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
    ?ts <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
    ?var2 <https://github.com/magbak/otit_swt#hasTimeseries> ?ts .
     }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_option_expression_filter_projection() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/otit_swt#>
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
    let (static_rewrite, _) = rewriter.rewrite_query(preprocessed_query).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?pv ?ts_datatype_0 ?ts_external_id_0 WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    OPTIONAL {
    ?var2 <https://example.com/hasPropertyValue> ?pv .
    ?ts <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
    ?ts <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
    ?var2 <https://github.com/magbak/otit_swt#hasTimeseries> ?ts .
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
    PREFIX qry:<https://github.com/magbak/otit_swt#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 ?pv WHERE {
        ?var1 a ?var2 .
        OPTIONAL {
            {
            ?var2 ex:hasPropertyValue ?pv .
            ?var2 qry:hasTimeseries ?ts .
            ?ts qry:hasDataPoint ?dp .
            ?dp qry:hasValue ?val .
            ?dp qry:hasTimestamp ?t .
            FILTER(?val <= 0.5 && !(?pv))
            } UNION {
            ?var2 ex:hasPropertyValue ?pv .
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
    let (static_rewrite, _) = rewriter.rewrite_query(preprocessed_query).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?pv ?ts_datatype_0 ?ts_datatype_1 ?ts_external_id_0 ?ts_external_id_1 WHERE {
        ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
        OPTIONAL {
            {
              ?var2 <https://example.com/hasPropertyValue> ?pv .
              ?ts <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
              ?ts <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
              ?var2 <https://github.com/magbak/otit_swt#hasTimeseries> ?ts .
              FILTER(!?pv)
            }
            UNION {
              ?var2 <https://example.com/hasPropertyValue> ?pv .
              ?ts <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_1 .
              ?ts <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_1 .
              ?var2 <https://github.com/magbak/otit_swt#hasTimeseries> ?ts .
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
    PREFIX qry:<https://github.com/magbak/otit_swt#>
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
    let (static_rewrite, _) = rewriter.rewrite_query(preprocessed_query).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_datatype_0 ?ts_datatype_1 ?ts_external_id_0 ?ts_external_id_1 WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?ts1 <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
    ?ts1 <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
    ?var1 <https://github.com/magbak/otit_swt#hasTimeseries> ?ts1 .
    ?ts2 <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_1 .
    ?ts2 <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_1 .
    ?var2 <https://github.com/magbak/otit_swt#hasTimeseries> ?ts2 . }
    "#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_fix_dropped_triple() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?t ?v WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s otit_swt:hasTimeseries ?ts .
        ?ts otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasTimestamp ?t .
        ?dp otit_swt:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 50) .
    }"#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrite, time_series_queries) = rewriter.rewrite_query(preprocessed_query).unwrap();
    let expected_str = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?ts_datatype_0 ?ts_external_id_0 WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?ts otit_swt:hasExternalId ?ts_external_id_0 .
        ?ts otit_swt:hasDatatype ?ts_datatype_0 .
        ?s otit_swt:hasTimeseries ?ts .
    }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, expected_query);

    let expected_time_series_queries = vec![BasicTimeSeriesQuery {
        identifier_variable: Some(Variable::new_unchecked("ts_external_id_0")),
        timeseries_variable: Some(VariableInContext::new(
            Variable::new_unchecked("ts"),
            Context::from_path(vec![
                PathEntry::ProjectInner,
                PathEntry::FilterInner,
                PathEntry::BGP,
            ]),
        )),
        data_point_variable: Some(VariableInContext::new(
            Variable::new_unchecked("dp"),
            Context::from_path(vec![
                PathEntry::ProjectInner,
                PathEntry::FilterInner,
                PathEntry::BGP,
            ]),
        )),
        value_variable: Some(VariableInContext::new(
            Variable::new_unchecked("v"),
            Context::from_path(vec![
                PathEntry::ProjectInner,
                PathEntry::FilterInner,
                PathEntry::BGP,
            ]),
        )),
        datatype_variable: Some(Variable::new_unchecked("ts_datatype_0")),
        datatype: None,
        timestamp_variable: Some(VariableInContext::new(
            Variable::new_unchecked("t"),
            Context::from_path(vec![
                PathEntry::ProjectInner,
                PathEntry::FilterInner,
                PathEntry::BGP,
            ]),
        )),
        ids: None,
    }];
    assert_eq!(time_series_queries, expected_time_series_queries);
}

#[test]
fn test_property_path_expression() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/magbak/otit_swt#>
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
    let (static_rewrite, time_series_queries) = rewriter.rewrite_query(preprocessed_query).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_datatype_0 ?ts_datatype_1 ?ts_external_id_0 ?ts_external_id_1 WHERE {
     ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
     ?blank_replacement_0 <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
     ?blank_replacement_0 <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
     ?var1 <https://github.com/magbak/otit_swt#hasTimeseries> ?blank_replacement_0 .
     ?blank_replacement_1 <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_1 .
     ?blank_replacement_1 <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_1 .
     ?var2 <https://github.com/magbak/otit_swt#hasTimeseries> ?blank_replacement_1 . }
    "#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    let expected_time_series_queries = vec![
        BasicTimeSeriesQuery {
            identifier_variable: Some(Variable::new_unchecked("ts_external_id_0")),
            timeseries_variable: Some(VariableInContext::new(
                Variable::new_unchecked("blank_replacement_0"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            data_point_variable: Some(VariableInContext::new(
                Variable::new_unchecked("dp1"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            value_variable: Some(VariableInContext::new(
                Variable::new_unchecked("val1"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            datatype_variable: Some(Variable::new_unchecked("ts_datatype_0")),
            datatype: None,
            timestamp_variable: Some(VariableInContext::new(
                Variable::new_unchecked("t"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            ids: None,
        },
        BasicTimeSeriesQuery {
            identifier_variable: Some(Variable::new_unchecked("ts_external_id_1")),
            timeseries_variable: Some(VariableInContext::new(
                Variable::new_unchecked("blank_replacement_1"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            data_point_variable: Some(VariableInContext::new(
                Variable::new_unchecked("dp2"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            value_variable: Some(VariableInContext::new(
                Variable::new_unchecked("val2"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            datatype_variable: Some(Variable::new_unchecked("ts_datatype_1")),
            datatype: None,
            timestamp_variable: Some(VariableInContext::new(
                Variable::new_unchecked("t"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            ids: None,
        },
    ];
    assert_eq!(time_series_queries, expected_time_series_queries);
    assert_eq!(static_rewrite, expected_query);
}

#[test]
fn test_having_query() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s otit_swt:hasTimeseries ?ts .
        ?ts otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasTimestamp ?t .
        ?dp otit_swt:hasValue ?v .
        BIND(FLOOR(seconds(?t) / 5.0) as ?second_5)
        BIND(minutes(?t) AS ?minute)
        BIND(hours(?t) AS ?hour)
        BIND(day(?t) AS ?day)
        BIND(month(?t) AS ?month)
        BIND(year(?t) AS ?year)
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?year ?month ?day ?hour ?minute ?second_5
    HAVING (SUM(?v) > 1000)
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrite, _) = rewriter.rewrite_query(preprocessed_query).unwrap();
    let expected_str = r#"
    SELECT ?w ?ts_datatype_0 ?ts_external_id_0 WHERE {
    ?w <http://example.org/types#hasSensor> ?s .
    ?ts <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
    ?ts <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
    ?s <https://github.com/magbak/otit_swt#hasTimeseries> ?ts .
    }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(expected_query, static_rewrite);
    //println!("{}", static_rewrite);
}

#[test]
fn test_exists_query() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s WHERE {
        ?w types:hasSensor ?s .
        FILTER EXISTS {SELECT ?s WHERE {
            ?s otit_swt:hasTimeseries ?ts .
            ?ts otit_swt:hasDataPoint ?dp .
            ?dp otit_swt:hasTimestamp ?t .
            ?dp otit_swt:hasValue ?v .
            FILTER(?v > 300)}}
    }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrite, _) = rewriter.rewrite_query(preprocessed_query).unwrap();
    let expected_str = r#"
    SELECT ?w ?s ?ts ?ts_datatype_0 ?ts_external_id_0 WHERE {
    ?w <http://example.org/types#hasSensor> ?s .
    OPTIONAL {
            ?ts <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
            ?ts <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
            ?s <https://github.com/magbak/otit_swt#hasTimeseries> ?ts . } }
    "#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(expected_query, static_rewrite);
    //println!("{}", static_rewrite);
}

#[test]
fn test_filter_lost_bug() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit:<https://github.com/magbak/otit_swt#>
    PREFIX wp:<https://github.com/magbak/otit_swt/windpower_example#>
    PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rds:<https://github.com/magbak/otit_swt/rds_power#>
    SELECT ?site_label ?wtur_label ?ts ?val ?t WHERE {
    ?site a rds:Site .
    ?site rdfs:label ?site_label .
    ?site rds:hasFunctionalAspect ?wtur_asp .
    ?wtur_asp rdfs:label ?wtur_label .
    ?wtur rds:hasFunctionalAspectNode ?wtur_asp .
    ?wtur rds:hasFunctionalAspect ?gensys_asp .
    ?gensys rds:hasFunctionalAspectNode ?gensys_asp .
    ?gensys otit:hasTimeseries ?ts .
    ?ts rdfs:label "Production" .
    ?ts otit:hasDataPoint ?dp .
    ?dp otit:hasValue ?val .
    ?dp otit:hasTimestamp ?t .
    FILTER(?wtur_label = "A1" && ?t > "2022-06-17T08:46:53"^^xsd:dateTime) .
}"#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrite, _) = rewriter.rewrite_query(preprocessed_query).unwrap();

    let expected_str = r#"
    SELECT ?site_label ?wtur_label ?ts ?ts_datatype_0 ?ts_external_id_0 WHERE {
    ?site <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://github.com/magbak/otit_swt/rds_power#Site> .
    ?site <http://www.w3.org/2000/01/rdf-schema#label> ?site_label .
    ?site <https://github.com/magbak/otit_swt/rds_power#hasFunctionalAspect> ?wtur_asp .
    ?wtur_asp <http://www.w3.org/2000/01/rdf-schema#label> ?wtur_label .
    ?wtur <https://github.com/magbak/otit_swt/rds_power#hasFunctionalAspectNode> ?wtur_asp .
    ?wtur <https://github.com/magbak/otit_swt/rds_power#hasFunctionalAspect> ?gensys_asp .
    ?gensys <https://github.com/magbak/otit_swt/rds_power#hasFunctionalAspectNode> ?gensys_asp .
    ?ts <https://github.com/magbak/otit_swt#hasExternalId> ?ts_external_id_0 .
    ?ts <https://github.com/magbak/otit_swt#hasDatatype> ?ts_datatype_0 .
    ?gensys <https://github.com/magbak/otit_swt#hasTimeseries> ?ts .
    ?ts <http://www.w3.org/2000/01/rdf-schema#label> "Production" .
    FILTER((?wtur_label = "A1"))
    }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(expected_query, static_rewrite);
}
