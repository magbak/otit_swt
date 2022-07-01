use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::str::FromStr;
use nom::Finish;
use oxrdf::{BlankNode, NamedNode, Subject, Term, Triple};
use polars::frame::DataFrame;
use rstest::*;
use polars::series::Series;
use mapper::mapping::Mapping;
use mapper::parser::stottr_doc;
use mapper::resolver::resolve_document;
use mapper::templates::TemplateDataset;
use rio_api::parser::TriplesParser;
use rio_turtle::TurtleError;


#[fixture]
fn testdata_path() -> PathBuf {
    let manidir = env!("CARGO_MANIFEST_DIR");
    let mut testdata_path = PathBuf::new();
    testdata_path.push(manidir);
    testdata_path.push("tests");
    testdata_path.push("mapper_testdata");
    testdata_path
}

#[rstest]
fn test_mapper_easy_case(testdata_path:PathBuf) {
    let t_str = r#"
    @prefix ex:<http://example.net/ns#>.

    ex:ExampleTemplate [?myVar1 , ?myVar2]
      :: {
        ottr:Triple(ex:anObject, ex:hasNumberString, ?myVar1) ,
        ottr:Triple(ex:anObject, ex:hasOtherNumberString, ?myVar2)
      } .
    "#;
    let (_,doc) = stottr_doc(t_str).finish().expect("Ok");
    let doc = resolve_document(doc).expect("Resolution problem");
    let template_dataset = TemplateDataset::new(vec![doc]).expect("Dataset problem");

    let mut k = Series::from_iter(["KeyOne", "KeyTwo"]);
    k.rename("Key");
    let mut v1 = Series::from_iter(&[1, 2i32]);
    v1.rename("myVar1");
    let mut v2 = Series::from_iter(&[3,4i32]);
    v2.rename("myVar2");
    let series = [k, v1, v2];
    let df = DataFrame::from_iter(series);

    let mut mapping = Mapping::new(&template_dataset);
    let report = mapping.expand(&NamedNode::new_unchecked("http://example.net/ns#ExampleTemplate"), df).expect("");
    let mut file_path = testdata_path.clone();
    file_path.push("easy_case.ttl");
    // let mut file = File::create(file_path.as_path()).expect("could not open file");
    // mapping.write_n_triples(&mut file);
    let mut file = File::open(file_path.as_path()).expect("Could not open file");
    let mut reader = BufReader::new(file);
    let mut triples = vec![];

    rio_turtle::NTriplesParser::new(&mut reader).parse_all(&mut |x| {
        let subject = match x.subject {
             rio_api::model::Subject::NamedNode(nn) => {Subject::NamedNode(NamedNode::new_unchecked(nn.to_string()))}
             rio_api::model::Subject::BlankNode(bn) => {Subject::BlankNode(BlankNode::new_unchecked(bn.to_string()))}
             rio_api::model::Subject::Triple(_) => {panic!("Not supported")}
        };
        let predicate = NamedNode::new_unchecked(x.predicate.to_string());
        let object = Term::from_str(&x.object.to_string()).unwrap();
        let t = Triple {
            subject,
            predicate,
            object
        };
        triples.push(t);
        Ok(()) as Result<(), TurtleError>
    }).expect("No problems");
    println!("{:?}", triples);
}