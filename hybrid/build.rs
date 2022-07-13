fn main() {
    let proto_files = &["../proto/FlightSql.proto"];
    let dep_dirs = &["../proto"];
    tonic_build::configure()
        .build_client(true)
        .compile(proto_files, dep_dirs)
        .expect("Building protos failed");
}
