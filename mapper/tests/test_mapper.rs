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
use polars::export::arrow::array::PrimitiveArray;
use polars_core::datatypes::DataType;
use polars_core::prelude::{NamedFrom, UInt16Chunked, UInt8Chunked};

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
    assert_eq!(expected_triples_set, actual_triples_set);
}

// ?Date
// ?Datetime_sec_tz
// ?Time
// ?Duration_sec
// ?Categorical
// ?List_Utf8

#[rstest]
fn test_derived_datatypes() {
    let stottr = r#"
@prefix ex:<http://example.net/ns#>.
ex:ExampleTemplate [
?Boolean,
?UInt32,
?UInt64,
?Int32,
?Int64,
?Float32,
?Float64,
?Utf8
] :: {
    ottr:Triple(ex:anObject, ex:hasVal, ?Boolean),
    ottr:Triple(ex:anObject, ex:hasVal, ?UInt32),
    ottr:Triple(ex:anObject, ex:hasVal, ?UInt64),
    ottr:Triple(ex:anObject, ex:hasVal, ?Int32),
    ottr:Triple(ex:anObject, ex:hasVal, ?Int64),
    ottr:Triple(ex:anObject, ex:hasVal, ?Float32),
    ottr:Triple(ex:anObject, ex:hasVal, ?Float64),
    ottr:Triple(ex:anObject, ex:hasVal, ?Utf8)
  } .
"#;
    let mut mapping = Mapping::from_str(&stottr).unwrap();
    let mut k = Series::from_iter(["KeyOne", "KeyTwo"]);
    k.rename("Key");
    let mut boolean = Series::from_iter(&[true, false]);
    boolean.rename("Boolean");
    let mut uint32 = Series::from_iter(&[5u32, 6u32]);
    uint32.rename("UInt32");
    let mut uint64 = Series::from_iter(&[7u64, 8u64]);
    uint64.rename("UInt64");
    let mut int32 = Series::from_iter(&[-13i32, -14i32]);
    int32.rename("Int32");
    let mut int64 = Series::from_iter(&[-15i64, -16i64]);
    int64.rename("Int64");
    let mut float32 = Series::from_iter(&[17.18f32, 19.20f32]);
    float32.rename("Float32");
    let mut float64 = Series::from_iter(&[21.22f64, 23.24f64]);
    float64.rename("Float64");
    let mut utf8 = Series::from_iter(["abcde", "fghij"]);
    utf8.rename("Utf8");


    let series = [k, boolean, uint32, uint64, int32, int64, float32, float64, utf8];
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
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "true",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "false",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "true",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "false",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "true",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "false",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "true",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "false",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "true",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "false",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}