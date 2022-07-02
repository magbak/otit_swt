extern crate core;

#[cfg(test)]
mod utils;

use crate::utils::triples_from_file;
use mapper::mapping::Mapping;
use oxrdf::{Literal, NamedNode, Subject, Term, Triple};
use polars::frame::DataFrame;
use polars::series::Series;
use rstest::*;
use std::collections::HashSet;
use std::fs::File;
use std::path::PathBuf;

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
fn test_mapper_easy_case(testdata_path: PathBuf) {
    let t_str = r#"
    @prefix ex:<http://example.net/ns#>.

    ex:ExampleTemplate [?myVar1 , ?myVar2]
      :: {
        ottr:Triple(ex:anObject, ex:hasNumberString, ?myVar1) ,
        ottr:Triple(ex:anObject, ex:hasOtherNumberString, ?myVar2)
      } .
    "#;

    let mut k = Series::from_iter(["KeyOne", "KeyTwo"]);
    k.rename("Key");
    let mut v1 = Series::from_iter(&[1, 2i32]);
    v1.rename("myVar1");
    let mut v2 = Series::from_iter(&[3, 4i32]);
    v2.rename("myVar2");
    let series = [k, v1, v2];
    let df = DataFrame::from_iter(series);

    let mut mapping = Mapping::from_str(&t_str).unwrap();
    let report = mapping
        .expand(
            &NamedNode::new_unchecked("http://example.net/ns#ExampleTemplate"),
            df,
        )
        .expect("");
    let mut actual_file_path = testdata_path.clone();
    actual_file_path.push("actual_easy_case.ttl");
    let mut actual_file = File::create(actual_file_path.as_path()).expect("could not open file");
    mapping.write_n_triples(&mut actual_file).unwrap();
    let actual_file = File::open(actual_file_path.as_path()).expect("Could not open file");
    let actual_triples = triples_from_file(actual_file);

    let mut expected_file_path = testdata_path.clone();
    expected_file_path.push("expected_easy_case.ttl");
    let expected_file = File::open(expected_file_path.as_path()).expect("Could not open file");
    let expected_triples = triples_from_file(expected_file);
    assert_eq!(expected_triples, actual_triples);
}

#[rstest]
fn test_nested_templates() {
    let stottr = r#"
@prefix ex:<http://example.net/ns#>.
ex:ExampleTemplate [?myVar1 , ?myVar2] :: {
    ex:Nested(?myVar1),  
    ottr:Triple(ex:anObject, ex:hasOtherNumber, ?myVar2)
  } .
ex:Nested [?myVar] :: {
    ottr:Triple(ex:anObject, ex:hasNumber, ?myVar)
} .
"#;
    let mut mapping = Mapping::from_str(&stottr).unwrap();
    let mut k = Series::from_iter(["KeyOne", "KeyTwo"]);
    k.rename("Key");
    let mut v1 = Series::from_iter(&[1, 2i32]);
    v1.rename("myVar1");
    let mut v2 = Series::from_iter(&[3, 4i32]);
    v2.rename("myVar2");
    let series = [k, v1, v2];
    let df = DataFrame::from_iter(series);
    let report = mapping
        .expand(
            &NamedNode::new_unchecked("http://example.net/ns#ExampleTemplate"),
            df,
        )
        .unwrap();
    let triples = mapping.to_triples();
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples.into_iter());
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "3",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "4",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
    ]);
}
