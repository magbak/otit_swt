use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;
use bollard::container::{Config, CreateContainerOptions, KillContainerOptions, ListContainersOptions, RemoveContainerOptions, StartContainerOptions};
use bollard::models::{ContainerConfig, ContainerSummary, HostConfig, Mount, PortBinding, PortMap};
use bollard::Docker;
use reqwest::header::CONTENT_TYPE;
use rstest::*;
use spargebra::{Query, Update};
use tokio::task;
use tokio::time::sleep;

const OXIGRAPH_SERVER_IMAGE: &str = "oxigraph/oxigraph:v0.3.2";
const QUERY_ENDPOINT: &str = "http://localhost:7878/query";
const UPDATE_ENDPOINT: &str = "http://localhost:7878/update";


async fn find_container(docker: &Docker, container_name: &str) -> Option<ContainerSummary> {
    let list = docker.list_containers(Some(ListContainersOptions::<String>{
        all: true,
        ..Default::default()
    })).await.expect("List containers problem");
    let slashed_container_name = "/".to_string() + container_name;
    let existing = list.iter().find(|cs| cs.names.is_some() && cs.names.as_ref().unwrap().iter().find(|n|n == &&slashed_container_name).is_some()).cloned();
    existing
}

#[fixture]
async fn sparql_endpoint() {
    let docker = Docker::connect_with_local_defaults().expect("Could not find local docker");
    let container_name = "my-oxigraph-server";
    let existing = find_container(&docker, container_name).await;
    if let Some(container) = existing {
        docker.remove_container(container_name, Some(RemoveContainerOptions{
            force: true,
            ..Default::default()
        })).await.expect("Remove existing problem");
    }
        let options = CreateContainerOptions {
            name: container_name,
        };
        let config = Config {
            image: Some(OXIGRAPH_SERVER_IMAGE),
            cmd: Some(vec!["--location", "/data", "serve", "--bind", "0.0.0.0:7878"]),
            exposed_ports: Some(HashMap::from([("7878/tcp", HashMap::new())])),
            host_config: Some(HostConfig{
                port_bindings: Some(HashMap::from([("7878/tcp".to_string(), Some(vec![PortBinding{ host_ip: None, host_port: Some("7878/tcp".to_string()) }]) )])),
                ..Default::default()
            }),
            ..Default::default()
        };
        docker
            .create_container(Some(options), config).await
            .expect("Problem creating container");
        docker.start_container(container_name, None::<StartContainerOptions<String>>).await.expect("Started container problem ");
        sleep(Duration::from_secs(10)).await;
        let created = find_container(&docker, container_name).await;
        assert!(created.is_some());
        assert!(created.as_ref().unwrap().status.as_ref().unwrap().contains("Up"));
        println!("{:?}", created);
}

#[fixture]
async fn with_testdata(#[future] sparql_endpoint: ()) {
    let _ = sparql_endpoint.await;
    let manidir = env!("CARGO_MANIFEST_DIR");
    let mut path_here = PathBuf::new();
    path_here.push(manidir);
    path_here.push("tests");
    path_here.push("testdata.sparql");
    let testdata_update_string = fs::read_to_string(path_here.as_path()).expect("Read testdata.sparql problem");

    let client = reqwest::Client::new();
    let put_request = client.post(UPDATE_ENDPOINT).header(CONTENT_TYPE, "application/sparql-update").body(testdata_update_string);
    let put_response = put_request.send().await.expect("Update error");
    println!("{:?}", put_response)
}

#[rstest]
#[tokio::test]
async fn test_full_case(#[future] with_testdata:()) {
    let _ = with_testdata.await;
    let client = reqwest::Client::new();
    let response = client.post(QUERY_ENDPOINT).header(CONTENT_TYPE, "application/sparql-query").body("SELECT * WHERE {?a ?b ?c }").send().await.expect("postproblem");
    println!("{:?}", response);
    println!("Hello");
}



