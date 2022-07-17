mod common;

use crate::common::{add_sparql_testdata, find_container, start_sparql_container, QUERY_ENDPOINT};
use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, RemoveContainerOptions,
    StartContainerOptions,
};
use bollard::models::{HostConfig, PortBinding};
use bollard::Docker;
use hybrid::orchestrator::execute_hybrid_query;
use hybrid::simple_in_memory_timeseries::InMemoryTimeseriesDatabase;
use hybrid::splitter::parse_sparql_select_query;
use hybrid::static_sparql::execute_sparql_query;
use hybrid::timeseries_database::arrow_flight_sql_database::ArrowFlightSQLDatabase;
use hybrid::timeseries_database::timeseries_sql_rewrite::TimeSeriesTable;
use log::debug;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Term, Variable};
use polars::prelude::{CsvReader, SerReader};
use polars_core::datatypes::AnyValue;
use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, StatusCode};
use rstest::*;
use serial_test::serial;
use sparesults::QuerySolution;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;
use bollard::image::BuildImageOptions;
use tokio::time::sleep;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};

const DREMIO_SERVER_IMAGE: &str = "dremio/dremio-oss:22.0.0";
const ARROW_SQL_DATABASE_ENDPOINT: &str = "http://127.0.0.1:32010";
const DREMIO_ORIGIN: &str = "http://127.0.0.1:9047";

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
    testdata_path.push("query_execution_arrow_sql_testdata");
    testdata_path
}

#[fixture]
fn dockerfile_tar_gz_path() -> PathBuf {
    let manidir = env!("CARGO_MANIFEST_DIR");
    let mut dockerfile_path = PathBuf::new();
    dockerfile_path.push(manidir);
    dockerfile_path.push("tests");
    dockerfile_path.push("dremio_docker.tar.gz");
    dockerfile_path
}

#[fixture]
async fn sparql_endpoint() {
    start_sparql_container().await;
}

#[fixture]
async fn with_testdata(#[future] sparql_endpoint: (), mut testdata_path: PathBuf) {
    let _ = sparql_endpoint.await;
    testdata_path.push("testdata.sparql");
    add_sparql_testdata(testdata_path).await;
}

#[fixture]
async fn arrow_sql_endpoint(dockerfile_tar_gz_path: PathBuf) {
    println!("arrow");
    let docker = Docker::connect_with_local_defaults().expect("Could not find local docker");
    let container_name = "my-dremio-server";
    let existing = find_container(&docker, container_name).await;
    if let Some(_) = existing {
        docker
            .remove_container(
                container_name,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("Remove existing problem");
    }
    let mut file = File::open(dockerfile_tar_gz_path).unwrap();
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).unwrap();
    let mut build_stream = docker.build_image(BuildImageOptions {
        dockerfile: "Dockerfile",
        t: "my_dremio",
        ..Default::default()
    }, None, Some(contents.into()));
    while let Some(msg) = build_stream.next().await {
        println!("Message: {:?}", msg);
    }

    let options = CreateContainerOptions {
        name: container_name,
    };
    let config = Config {
        image: Some("my_dremio"),
        cmd: None,
        exposed_ports: Some(HashMap::from([
            ("9047/tcp", HashMap::new()),
            ("32010/tcp", HashMap::new()),
        ])),
        host_config: Some(HostConfig {
            port_bindings: Some(HashMap::from([
                (
                    "9047/tcp".to_string(),
                    Some(vec![PortBinding {
                        host_ip: None,
                        host_port: Some("9047/tcp".to_string()),
                    }]),
                ),
                (
                    "32010/tcp".to_string(),
                    Some(vec![PortBinding {
                        host_ip: None,
                        host_port: Some("32010/tcp".to_string()),
                    }]),
                ),
            ])),
            ..Default::default()
        }),
        ..Default::default()
    };
    docker
        .create_container(Some(options), config)
        .await
        .expect("Problem creating container");
    docker
        .start_container(container_name, None::<StartContainerOptions<String>>)
        .await
        .expect("Started container problem ");
    sleep(Duration::from_secs(30)).await;
    let created = find_container(&docker, container_name).await;
    assert!(created.is_some());

    assert!(created
        .as_ref()
        .unwrap()
        .status
        .as_ref()
        .unwrap()
        .contains("Up"));
}

#[fixture]
async fn with_sparql_testdata(#[future] sparql_endpoint: (), mut testdata_path: PathBuf) {
    let _ = sparql_endpoint.await;
    testdata_path.push("testdata.sparql");
    add_sparql_testdata(testdata_path).await;
}

#[fixture]
fn timeseries_table() -> TimeSeriesTable {
    TimeSeriesTable {
        time_series_table: r#""my_nas"."ts.parquet""#.to_string(),
        value_column: "v".to_string(),
        timestamp_column: "ts".to_string(),
        identifier_column: "id".to_string(),
        value_datatype: xsd::UNSIGNED_INT.into_owned(),
    }
}

async fn ts_sql_db(
    timeseries_table: TimeSeriesTable,
) -> ArrowFlightSQLDatabase {
    ArrowFlightSQLDatabase::new(ARROW_SQL_DATABASE_ENDPOINT, vec![timeseries_table])
        .await
        .unwrap()
}

#[derive(Deserialize)]
struct Token {
    pub token: String,
}

#[derive(Serialize)]
struct UserPass {
    pub userName: String,
    pub password: String,
}

#[derive(Serialize)]
struct NewSource {
    pub entityType: String,
    pub name: String,
    #[serde(rename="type")]
    pub sourcetype: String,
    pub config: NasConfig,
}

#[derive(Serialize)]
struct NasConfig {
    pub path: String,
}

#[fixture]
async fn with_timeseries_testdata(#[future] arrow_sql_endpoint: ()) {
    let _ = arrow_sql_endpoint.await;
    let mut c = reqwest::Client::new();
    let mut bld = c.request(Method::POST, format!("{}/apiv2/login", DREMIO_ORIGIN));
    let user_pass = UserPass {userName:"dremio".to_string(), password:"dremio123".to_string()};
    bld = bld.header(CONTENT_TYPE, "application/json");
    bld = bld.json(&user_pass);
    let res = bld.send().await.unwrap();
    let token = res.json::<Token>().await.unwrap();
    println!("Token: {}", token.token);

    //Add source
    let mut bld = c.request(Method::POST, format!("{}/api/v3/catalog", DREMIO_ORIGIN));
    bld = bld.bearer_auth(token.token.clone());
    let create = r#"
    {
  "entityType": "source",
  "config": {
    "path": "/var/dremio-data"
  },
  "type": "NAS",
  "name": "my_nas",
  "metadataPolicy": {
    "authTTLMs": 86400000,
    "namesRefreshMs": 3600000,
    "datasetRefreshAfterMs": 3600000,
    "datasetExpireAfterMs": 10800000,
    "datasetUpdateMode": "PREFETCH_QUERIED",
    "deleteUnavailableDatasets": true,
    "autoPromoteDatasets": false
  },
  "accelerationGracePeriodMs": 10800000,
  "accelerationRefreshPeriodMs": 3600000,
  "accelerationNeverExpire": false,
  "accelerationNeverRefresh": false
}
    "#;
    bld = bld.body(create);
    bld = bld.header(CONTENT_TYPE, "application/json");
    let resp = bld.send().await.unwrap().text().await.unwrap();
    println!("Resp {:?}", resp);
}


#[rstest]
#[tokio::test]
#[serial]
async fn test_simple_hybrid_query(
    #[future] with_sparql_testdata: (),
    #[future] with_timeseries_testdata: (),
    timeseries_table: TimeSeriesTable,
    testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_sparql_testdata.await;
    let _ = with_timeseries_testdata.await;
    let db = ts_sql_db(timeseries_table).await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX quarry:<https://github.com/magbak/quarry-rs#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?t ?v WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s quarry:hasTimeseries ?ts .
        ?ts quarry:hasDataPoint ?dp .
        ?dp quarry:hasTimestamp ?t .
        ?dp quarry:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 200) .
    }
    "#;
    let df = execute_hybrid_query(query, QUERY_ENDPOINT, Box::new(db))
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
