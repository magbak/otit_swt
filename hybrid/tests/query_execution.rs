mod common;

use hybrid::engine::Engine;
use hybrid::pushdown_setting::all_pushdowns;
use hybrid::splitter::parse_sparql_select_query;
use hybrid::static_sparql::execute_sparql_query;
use hybrid::timeseries_database::simple_in_memory_timeseries::InMemoryTimeseriesDatabase;
use log::debug;
use oxrdf::{NamedNode, Term, Variable};
use polars::prelude::{CsvReader, SerReader};
use rstest::*;
use serial_test::serial;
use sparesults::QuerySolution;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

use crate::common::{
    add_sparql_testdata, compare_all_solutions, start_sparql_container, QUERY_ENDPOINT,
};

#[fixture]
fn use_logger() {
    let res = env_logger::try_init();
    match res {
        Ok(_) => {}
        Err(_) => {
            debug!("Tried to initialize logger which is already initialize")
        }
    }
}

#[fixture]
fn testdata_path() -> PathBuf {
    let manidir = env!("CARGO_MANIFEST_DIR");
    let mut testdata_path = PathBuf::new();
    testdata_path.push(manidir);
    testdata_path.push("tests");
    testdata_path.push("query_execution_testdata");
    testdata_path
}

#[fixture]
async fn sparql_endpoint() {
    start_sparql_container().await
}

#[fixture]
async fn with_testdata(#[future] sparql_endpoint: (), testdata_path: PathBuf) {
    let _ = sparql_endpoint.await;
    let mut testdata_path = testdata_path.clone();
    testdata_path.push("testdata.sparql");
    add_sparql_testdata(testdata_path).await;
}

#[fixture]
fn inmem_time_series_database(testdata_path: PathBuf) -> InMemoryTimeseriesDatabase {
    let mut frames = HashMap::new();
    for t in ["ts1", "ts2"] {
        let mut file_path = testdata_path.clone();
        file_path.push(t.to_string() + ".csv");

        let file = File::open(file_path.as_path()).expect("could not open file");
        let df = CsvReader::new(file)
            .infer_schema(None)
            .has_header(true)
            .with_parse_dates(true)
            .finish()
            .expect("DF read error");
        frames.insert(t.to_string(), df);
    }
    InMemoryTimeseriesDatabase { frames }
}

#[fixture]
fn engine(inmem_time_series_database: InMemoryTimeseriesDatabase) -> Engine {
    Engine::new(all_pushdowns(), Box::new(inmem_time_series_database))
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_static_query(#[future] with_testdata: (), use_logger: ()) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = parse_sparql_select_query(
        r#"
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    SELECT * WHERE {?a otit_swt:hasTimeseries ?b }
    "#,
    )
    .unwrap();
    let query_solns = execute_sparql_query(QUERY_ENDPOINT, &query).await.unwrap();
    let expected_solutions = vec![
        QuerySolution::from((
            vec![Variable::new("a").unwrap(), Variable::new("b").unwrap()],
            vec![
                Some(Term::NamedNode(
                    NamedNode::new("http://example.org/case#mySensor2").unwrap(),
                )),
                Some(Term::NamedNode(
                    NamedNode::new("http://example.org/case#myTimeseries2").unwrap(),
                )),
            ],
        )),
        QuerySolution::from((
            vec![Variable::new("a").unwrap(), Variable::new("b").unwrap()],
            vec![
                Some(Term::NamedNode(
                    NamedNode::new("http://example.org/case#mySensor1").unwrap(),
                )),
                Some(Term::NamedNode(
                    NamedNode::new("http://example.org/case#myTimeseries1").unwrap(),
                )),
            ],
        )),
    ];
    compare_all_solutions(expected_solutions, query_solns);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_simple_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
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
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 200) .
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_simple_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_complex_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w1 ?w2 ?t ?v1 ?v2 WHERE {
        ?w1 a types:BigWidget .
        ?w2 a types:SmallWidget .
        ?w1 types:hasSensor ?s1 .
        ?w2 types:hasSensor ?s2 .
        ?s1 otit_swt:hasTimeseries ?ts1 .
        ?s2 otit_swt:hasTimeseries ?ts2 .
        ?ts1 otit_swt:hasDataPoint ?dp1 .
        ?ts2 otit_swt:hasDataPoint ?dp2 .
        ?dp1 otit_swt:hasTimestamp ?t .
        ?dp2 otit_swt:hasTimestamp ?t .
        ?dp1 otit_swt:hasValue ?v1 .
        ?dp2 otit_swt:hasValue ?v2 .
        FILTER(?t > "2022-06-01T08:46:55"^^xsd:dateTime && ?v1 < ?v2) .
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_complex_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_pushdown_group_by_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s otit_swt:hasTimeseries ?ts .
        ?ts otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasTimestamp ?t .
        ?dp otit_swt:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime) .
    } GROUP BY ?w
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["w"], vec![false])
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["w"], vec![false])
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_pushdown_group_by_second_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s otit_swt:hasTimeseries ?ts .
        ?ts otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasTimestamp ?t .
        ?dp otit_swt:hasValue ?v .
        BIND(seconds(?t) as ?second)
        BIND(minutes(?t) AS ?minute)
        BIND(hours(?t) AS ?hour)
        BIND(day(?t) AS ?day)
        BIND(month(?t) AS ?month)
        BIND(year(?t) AS ?year)
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?year ?month ?day ?hour ?minute ?second
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["w", "sum_v"], vec![false])
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_second_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["w", "sum_v"], vec![false])
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_pushdown_group_by_second_having_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (CONCAT(?year, "-", ?month, "-", ?day, "-", ?hour, "-", ?minute, "-", (?second_5*5)) as ?period) (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s otit_swt:hasTimeseries ?ts .
        ?ts otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasTimestamp ?t .
        ?dp otit_swt:hasValue ?v .
        BIND(xsd:integer(FLOOR(seconds(?t) / 5.0)) as ?second_5)
        BIND(minutes(?t) AS ?minute)
        BIND(hours(?t) AS ?hour)
        BIND(day(?t) AS ?day)
        BIND(month(?t) AS ?month)
        BIND(year(?t) AS ?year)
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?year ?month ?day ?hour ?minute ?second_5
    HAVING (SUM(?v)>100)
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["w", "sum_v"], vec![false])
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_second_having_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["w", "sum_v"], vec![false])
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    //println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_pushdown_group_by_concat_agg_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?seconds_5 (GROUP_CONCAT(?v ; separator="-") as ?cc) WHERE {
        ?w types:hasSensor ?s .
        ?s otit_swt:hasTimeseries ?ts .
        ?ts otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasTimestamp ?t .
        ?dp otit_swt:hasValue ?v .
        BIND(xsd:integer(FLOOR(seconds(?t) / 5.0)) as ?seconds_5)
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?seconds_5
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["w", "seconds_5"], vec![false])
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_concat_agg_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["w", "seconds_5"], vec![false])
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    //println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_pushdown_groupby_exists_something_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?seconds_3 (AVG(?v) as ?mean) WHERE {
        ?w types:hasSensor ?s .
        ?s otit_swt:hasTimeseries ?ts .
        ?ts otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasTimestamp ?t .
        ?dp otit_swt:hasValue ?v .
        BIND(xsd:integer(FLOOR(seconds(?t) / 3.0)) as ?seconds_3)
        FILTER EXISTS {SELECT ?w WHERE {?w types:hasSomething ?smth}}
    } GROUP BY ?w ?seconds_3
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["w", "seconds_3"], vec![false])
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_exists_something_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["w", "seconds_3"], vec![false])
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_pushdown_groupby_exists_timeseries_value_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
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
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["w"], vec![false])
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_exists_timeseries_value_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["w"], vec![false])
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_pushdown_groupby_exists_aggregated_timeseries_value_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
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
            FILTER(?v < 300)}
            GROUP BY ?s
            HAVING (SUM(?v) >= 1000)
            }
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["w"], vec![false])
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_exists_aggregated_timeseries_value_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["w"], vec![false])
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_pushdown_groupby_not_exists_aggregated_timeseries_value_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s WHERE {
        ?w types:hasSensor ?s .
        FILTER NOT EXISTS {SELECT ?s WHERE {
            ?s otit_swt:hasTimeseries ?ts .
            ?ts otit_swt:hasDataPoint ?dp .
            ?dp otit_swt:hasTimestamp ?t .
            ?dp otit_swt:hasValue ?v .
            FILTER(?v < 300)}
            GROUP BY ?s
            HAVING (SUM(?v) <= 1000)
            }
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["w"], vec![false])
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_not_exists_aggregated_timeseries_value_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["w"], vec![false])
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_path_group_by_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (MAX(?v) as ?max_v) WHERE {
        ?w types:hasSensor/otit_swt:hasTimeseries/otit_swt:hasDataPoint/otit_swt:hasValue ?v .}
        GROUP BY ?w
        ORDER BY ASC(?max_v)
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_path_group_by_query.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_optional_clause_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?v ?greater WHERE {
        ?w types:hasSensor/otit_swt:hasTimeseries/otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasValue ?v .
        OPTIONAL {
        BIND(?v>300 as ?greater)
        FILTER(?greater)
        }
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_optional_clause_query.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_minus_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?v WHERE {
        ?w types:hasSensor/otit_swt:hasTimeseries/otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasValue ?v .
        MINUS {
        ?dp otit_swt:hasValue ?v .
        FILTER(?v > 300)
        }
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["w", "v"], vec![false])
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_minus_query.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["w", "v"], vec![false])
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_in_expression_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?v WHERE {
        ?w types:hasSensor/otit_swt:hasTimeseries/otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasValue ?v .
        FILTER(?v IN ((300+4), (304-3), 307))
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_in_expression.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_values_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?v WHERE {
        ?w types:hasSensor/otit_swt:hasTimeseries/otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasValue ?v .
        VALUES ?v2 { 301 304 307 }
        FILTER(?v = ?v2)
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_values_query.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_if_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (IF(?v>300,?v,300) as ?v_with_min) WHERE {
        ?w types:hasSensor/otit_swt:hasTimeseries/otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasValue ?v .
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["w", "v_with_min"], vec![false])
        .expect("Sort problem");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_if_query.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["w", "v_with_min"], vec![false])
        .expect("Sort problem");

    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_distinct_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT DISTINCT ?w (IF(?v>300,?v,300) as ?v_with_min) WHERE {
        ?w types:hasSensor/otit_swt:hasTimeseries/otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasValue ?v .
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_distinct_query.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_union_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?v WHERE {
        { ?w a types:BigWidget .
        ?w types:hasSensor/otit_swt:hasTimeseries/otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasValue ?v .
        FILTER(?v > 100) }
        UNION {
            ?w a types:SmallWidget .
            ?w types:hasSensor/otit_swt:hasTimeseries/otit_swt:hasDataPoint ?dp .
            ?dp otit_swt:hasValue ?v .
            FILTER(?v < 100)
        }
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["w", "v"], vec![false])
        .expect("Sort problem");

    let mut file_path = testdata_path.clone();
    file_path.push("expected_union_query.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["w", "v"], vec![false])
        .expect("Sort problem");

    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_coalesce_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?s1 ?t1 ?v1 ?v2 (COALESCE(?v2, ?v1) as ?c) WHERE {
        ?s1 otit_swt:hasTimeseries/otit_swt:hasDataPoint ?dp1 .
        ?dp1 otit_swt:hasValue ?v1 .
        ?dp1 otit_swt:hasTimestamp ?t1 .
        OPTIONAL {
        ?s1 otit_swt:hasTimeseries/otit_swt:hasDataPoint ?dp2 .
        ?dp2 otit_swt:hasValue ?v2 .
        ?dp2 otit_swt:hasTimestamp ?t2 .
        FILTER(seconds(?t2) >= (seconds(?t1) - 1) && seconds(?t2) <= (seconds(?t1) + 1) && ?v2 > ?v1)
        }
    }
    "#;
    let df = engine
        .execute_hybrid_query(query, QUERY_ENDPOINT)
        .await
        .expect("Hybrid error")
        .sort(&["s1", "t1", "v1", "v2"], vec![false])
        .expect("Sort problem");

    let mut file_path = testdata_path.clone();
    file_path.push("expected_coalesce_query.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(&["s1", "t1", "v1", "v2"], vec![false])
        .expect("Sort problem");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}
