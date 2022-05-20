use bollard::container::{Config, CreateContainerOptions};
use bollard::models::{ContainerConfig, HostConfig, Mount};
use bollard::Docker;
use rstest::*;
use tokio::task;

const OXIGRAPH_SERVER_IMAGE: &str = "oxigraph/oxigraph:v0.3.2";

#[fixture]
fn sparql_endpoint() {
    let docker = Docker::connect_with_local_defaults().expect("Could not find local docker");
    let container_name = "my-oxigraph-server";
    let options = CreateContainerOptions {
        name: container_name,
    };
    let config = Config {
        image: Some(OXIGRAPH_SERVER_IMAGE),
        cmd: Some(vec!["--location /data serve --bind 0.0.0.0:7878"]),
            ..Default::default()
        };
    docker
        .create_container(Some(options), config)
        .expect("Problem creating container");
    task::block_in_place(|| {docker.start_container(container_name, None).await.expect("Problem starting container")});
}




