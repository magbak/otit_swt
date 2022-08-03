mod common;
mod opcua_data_provider;

use rstest::*;
use serial_test::serial;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::{thread, time};
use std::collections::HashMap;
use std::fs::File;
use std::thread::{JoinHandle, sleep};
use log::debug;
use tokio::runtime::Builder;
use hybrid::orchestrator::execute_hybrid_query;
use hybrid::timeseries_database::opcua_history_read::OPCUAHistoryRead;
use opcua_server::prelude::*;
use polars::io::SerReader;
use polars::prelude::CsvReader;
use polars_core::frame::DataFrame;

use crate::opcua_data_provider::OPCUADataProvider;
use crate::common::{add_sparql_testdata, start_sparql_container, QUERY_ENDPOINT};


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
    testdata_path.push("query_execution_opcua");
    testdata_path
}

#[fixture]
fn sparql_endpoint() {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    let runtime = builder.build().unwrap();
    runtime.block_on(start_sparql_container());
}

#[fixture]
fn with_testdata(sparql_endpoint: (), testdata_path: PathBuf) {

    let mut testdata_path = testdata_path.clone();
    testdata_path.push("testdata.sparql");
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    let runtime = builder.build().unwrap();
    runtime.block_on(add_sparql_testdata(testdata_path));
}

#[fixture]
fn frames(testdata_path: PathBuf) -> HashMap<String, DataFrame> {
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
    frames
}

#[fixture]
fn opcua_server_fixture(frames: HashMap<String, DataFrame>) -> JoinHandle<()>{
    let port = 1234;
    let path = "/";
    //From https://github.com/locka99/opcua/blob/master/docs/server.md
    let server = ServerBuilder::new()
        .application_name("Server Name")
        .application_uri("urn:server_uri")
        .discovery_urls(vec![format!("opc.tcp://{}:{}{}", hostname().unwrap(), port, path).into()])
        .create_sample_keypair(true)
        .pki_dir("./pki-server")
        .discovery_server_url(None)
        .host_and_port(hostname().unwrap(), port)
        .endpoints(
            [
                ("", "/", SecurityPolicy::None, MessageSecurityMode::None, &[ANONYMOUS_USER_TOKEN_ID]),
            ].iter().map(|v| {
                (v.0.to_string(), ServerEndpoint::from((v.1, v.2, v.3, &v.4[..])))
            }).collect())
        .server().unwrap();
    {
        let server_state = server.server_state();
        let mut server_state = server_state.write().unwrap();
        server_state.set_historical_data_provider(Box::new(OPCUADataProvider{frames}))
    }
    let handle = thread::spawn(move || {server.run()});
    sleep(time::Duration::from_secs(2));
    handle
}

#[rstest]
#[serial]
#[ignore]
fn test_basic_query(with_testdata: (), use_logger: (), opcua_server_fixture:JoinHandle<()>) {
    let _ = with_testdata;
    let _ = use_logger;
    let port = 1234;
    let path = "/";

    let endpoint = format!("opc.tcp://{}:{}{}", hostname().unwrap(), port, path);
    println!("Endpoint: {}", endpoint);
    let mut opcua_tsdb = OPCUAHistoryRead::new(&endpoint, 1);
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
        FILTER(?t >= "2022-06-01T08:46:53"^^xsd:dateTime && ?t <= "2022-06-06T08:46:53"^^xsd:dateTime) .
    }
    "#;
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    let runtime = builder.build().unwrap();
    let df = runtime.block_on(execute_hybrid_query(
        query,
        QUERY_ENDPOINT,
        Box::new(&mut opcua_tsdb),
    ))
    .expect("Hybrid error");
    println!("DF: {}", df);
}