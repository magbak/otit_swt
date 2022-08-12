extern crate core;

#[cfg(test)]
mod utils;

use crate::utils::triples_from_file;
use mapper::mapping::{ExpandOptions, Mapping, MintingOptions, ResolveIRI, SuffixGenerator};
use oxrdf::{Literal, NamedNode, Subject, Term, Triple};
use polars::frame::DataFrame;
use polars::series::Series;
use polars_core::prelude::{AnyValue, NamedFrom, TimeUnit};
use rstest::*;
use serial_test::serial;
use std::collections::{HashMap, HashSet};
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
#[serial]
fn test_mapper_easy_case(testdata_path: PathBuf) {
    let t_str = r#"
    @prefix ex:<http://example.net/ns#>.

    ex:ExampleTemplate [?myVar1 , ?myVar2]
      :: {
        ottr:Triple(ex:anObject, ex:hasNumber, ?myVar1) ,
        ottr:Triple(ex:anObject, ex:hasOtherNumber, ?myVar2)
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
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            df,
            Default::default(),
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
#[serial]
fn test_all_iri_case() {
    let t_str = r#"
    @prefix ex:<http://example.net/ns#>.

    ex:ExampleTemplate [xsd:anyURI ?myVar1]
      :: {
        ottr:Triple(ex:anObject, ex:relatesTo, ?myVar1)
      } .
    "#;

    let mut k = Series::from_iter(["KeyOne", "KeyTwo"]);
    k.rename("Key");
    let mut v1 = Series::from_iter([
        "http://example.net/ns#OneThing",
        "http://example.net/ns#AnotherThing",
    ]);
    v1.rename("myVar1");
    let series = [k, v1];
    let df = DataFrame::from_iter(series);

    let mut mapping = Mapping::from_str(&t_str).unwrap();
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            df,
            Default::default(),
        )
        .expect("");
    let triples = mapping.export_oxrdf_triples();
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples.into_iter());
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/ns#OneThing")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesTo"),
            object: Term::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#AnotherThing",
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_string_language_tag_cases() {
    let t_str = r#"
    @prefix ex:<http://example.net/ns#>.

    ex:ExampleTemplate [?myString]
      :: {
        ottr:Triple(ex:anObject, ex:hasString, ?myString) ,
        ottr:Triple(ex:anotherObject, ex:hasString, ""@ar-SA)
      } .
    "#;

    let mut k = Series::from_iter(["KeyOne", "KeyTwo"]);
    k.rename("Key");
    let mut my_string = Series::from_iter(["one", "two"]);
    my_string.rename("myString");
    let series = [k, my_string];
    let df = DataFrame::from_iter(series);

    let mut mapping = Mapping::from_str(&t_str).unwrap();
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            df,
            ExpandOptions {
                language_tags: Some(HashMap::from([(
                    "myString".to_string(),
                    "bn-BD".to_string(),
                )])),
                ..Default::default()
            },
        )
        .expect("");
    let triples = mapping.export_oxrdf_triples();
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples.into_iter());
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasString"),
            object: Term::Literal(Literal::new_language_tagged_literal_unchecked(
                "one", "bn-BD",
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasString"),
            object: Term::Literal(Literal::new_language_tagged_literal_unchecked(
                "two", "bn-BD",
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#anotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasString"),
            object: Term::Literal(Literal::new_language_tagged_literal_unchecked("", "ar-SA")),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_const_list_case() {
    let t_str = r#"
    @prefix ex:<http://example.net/ns#>.

    ex:ExampleTemplate [xsd:anyURI ?var1]
      :: {
        cross | ottr:Triple(?var1, ex:hasNumber, ++(1,2))
      } .
    "#;

    let mut k = Series::from_iter(["KeyOne", "KeyTwo"]);
    k.rename("Key");
    let mut v1 = Series::from_iter([
        "http://example.net/ns#OneThing",
        "http://example.net/ns#AnotherThing",
    ]);
    v1.rename("var1");
    let series = [k, v1];
    let df = DataFrame::from_iter(series);

    let mut mapping = Mapping::from_str(&t_str).unwrap();
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            df,
            Default::default(),
        )
        .expect("");
    let triples = mapping.export_oxrdf_triples();
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples.into_iter());
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#OneThing")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#OneThing")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#AnotherThing",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#AnotherThing",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
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
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            df,
            Default::default(),
        )
        .unwrap();
    let triples = mapping.export_oxrdf_triples();
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples.into_iter());
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "3",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "4",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_mint_iri_templates() {
    let stottr = r#"
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?myIRI1, ?myIRI2] :: {
    ottr:Triple(?myIRI1, ex:relatesTo, ?myIRI2)
  } .
"#;
    let mut mapping = Mapping::from_str(&stottr).unwrap();
    let mut k = Series::from_iter(["KeyOne", "KeyTwo"]);
    k.rename("Key");
    let mut my_iri1 = Series::from_iter([
        "http://example.net/things#subject1".to_string(),
        "http://example.net/things#subject2".to_string(),
    ]);
    my_iri1.rename("myIRI1");
    let series = [k.clone(), my_iri1];
    let df = DataFrame::from_iter(series);
    let report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            df,
            ExpandOptions {
                mint_iris: Some(HashMap::from([(
                    "myIRI2".to_string(),
                    MintingOptions {
                        prefix: "http://example.net/things#".to_string(),
                        suffix_generator: SuffixGenerator::Numbering(3),
                        list_length: None,
                    },
                )])),
                ..Default::default()
            },
        )
        .unwrap();
    let expected_minted_iris_df = DataFrame::new(vec![
        k.clone(),
        Series::new(
            "myIRI2",
            ["http://example.net/things#3", "http://example.net/things#4"],
        ),
    ])
    .unwrap();
    assert_eq!(
        report.minted_iris.as_ref().unwrap(),
        &expected_minted_iris_df
    );

    let triples = mapping.export_oxrdf_triples();
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples.into_iter());
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#subject1",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#3")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#subject2",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#4")),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_path_column() {
    let stottr1 = r#"
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate1 [?myIRI1, ?myIRI2] :: {
    ottr:Triple(?myIRI1, ex:relatesTo, ?myIRI2)
    } ."#;

    let stottr2 = r#"
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate2 [?myIRI1, ?myIRI2] :: {
    ottr:Triple(?myIRI1, ex:relatesDifferentlyTo, ?myIRI2)
    } ."#;

    let mut mapping = Mapping::from_strs(vec![stottr1, stottr2]).unwrap();
    let mut k = Series::from_iter(["KeyOne", "KeyTwo"]);
    k.rename("Key");
    let mut my_iri1 = Series::from_iter([
        "http://example.net/things#subject1".to_string(),
        "http://example.net/things#subject2".to_string(),
    ]);
    my_iri1.rename("myIRI1");
    let series = [k.clone(), my_iri1];
    let df = DataFrame::from_iter(series);
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate1",
            df,
            ExpandOptions {
                mint_iris: Some(HashMap::from([(
                    "myIRI2".to_string(),
                    MintingOptions {
                        prefix: "http://example.net/things#".to_string(),
                        suffix_generator: SuffixGenerator::Numbering(3),
                        list_length: None,
                    },
                )])),
                ..Default::default()
            },
        )
        .unwrap();

    let mut my_iri1 = Series::from_iter([
        "http://example.net/things#otherSubject1".to_string(),
        "http://example.net/things#otherSubject2".to_string(),
    ]);
    my_iri1.rename("myIRI1");
    let mut k2 = Series::from_iter(["KeyOne2", "KeyTwo2"]);
    k2.rename("Key");
    let mut fk = k.clone();
    fk.rename("myIRI2ForeignKey");
    let series = [k2, my_iri1, fk];
    let df = DataFrame::from_iter(series);
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate2",
            df,
            ExpandOptions {
                resolve_iris: Some(HashMap::from([(
                    "myIRI2".to_string(),
                    ResolveIRI {
                        key_column: "myIRI2ForeignKey".into(),
                        template: "http://example.net/ns#ExampleTemplate1".to_string(),
                        argument: "myIRI2".to_string(),
                    },
                )])),
                ..Default::default()
            },
        )
        .unwrap();

    let triples = mapping.export_oxrdf_triples();
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples.into_iter());
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#subject1",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#3")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#subject2",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#4")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#otherSubject1",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesDifferentlyTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#3")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#otherSubject2",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesDifferentlyTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#4")),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_path_column_with_list() {
    let stottr1 = r#"
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate1 [?myIRI1, ?myIRI2] :: {
    ottr:Triple(?myIRI1, ex:relatesTo, ?myIRI2)
    } ."#;

    let stottr2 = r#"
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate2 [?myIRI1, ?myIRI2] :: {
    cross | ottr:Triple(?myIRI1, ex:relatesDifferentlyTo, ++?myIRI2)
    } ."#;

    let mut mapping = Mapping::from_strs(vec![stottr1, stottr2]).unwrap();
    let mut k = Series::from_iter(["KeyOne", "KeyTwo"]);
    k.rename("Key");
    let mut my_iri1 = Series::from_iter([
        "http://example.net/things#subject1".to_string(),
        "http://example.net/things#subject2".to_string(),
    ]);
    my_iri1.rename("myIRI1");
    let series = [k.clone(), my_iri1];
    let df = DataFrame::from_iter(series);
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate1",
            df,
            ExpandOptions {
                mint_iris: Some(HashMap::from([(
                    "myIRI2".to_string(),
                    MintingOptions {
                        prefix: "http://example.net/things#".to_string(),
                        suffix_generator: SuffixGenerator::Numbering(3),
                        list_length: None,
                    },
                )])),
                ..Default::default()
            },
        )
        .unwrap();

    let mut my_iri1 = Series::from_iter([
        "http://example.net/things#otherSubject1".to_string(),
        "http://example.net/things#otherSubject2".to_string(),
    ]);
    my_iri1.rename("myIRI1");
    let mut k2 = Series::from_iter(["KeyOne2", "KeyTwo2"]);
    k2.rename("Key");

    let mut fk = Series::from_iter(["KeyOne", "KeyTwo", "KeyOne", "KeyTwo"]);
    fk.rename("myIRI2ForeignKey");
    let grby = Series::new("by", [1, 1, 2, 2]);
    let mut df = DataFrame::new(vec![grby, fk]).unwrap();
    df = df.groupby_stable([&"by"]).unwrap().agg_list().unwrap();
    df.rename("myIRI2ForeignKey_agg_list", "myIRI2ForeignKey")
        .unwrap();

    let series = [k2, my_iri1, df.column("myIRI2ForeignKey").unwrap().clone()];
    let df = DataFrame::from_iter(series);
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate2",
            df,
            ExpandOptions {
                resolve_iris: Some(HashMap::from([(
                    "myIRI2".to_string(),
                    ResolveIRI {
                        key_column: "myIRI2ForeignKey".into(),
                        template: "http://example.net/ns#ExampleTemplate1".to_string(),
                        argument: "myIRI2".to_string(),
                    },
                )])),
                ..Default::default()
            },
        )
        .unwrap();

    let triples = mapping.export_oxrdf_triples();
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples.into_iter());
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#subject1",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#3")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#subject2",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#4")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#otherSubject1",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesDifferentlyTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#3")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#otherSubject1",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesDifferentlyTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#4")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#otherSubject2",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesDifferentlyTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#3")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/things#otherSubject2",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesDifferentlyTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/things#4")),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

// ?Date
// ?Time
// ?Duration_sec
// ?List_Utf8

#[rstest]
#[serial]
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
?Utf8,
?Datetime_ms_tz,
?Datetime_ms
] :: {
    ottr:Triple(ex:anObject, ex:hasVal, ?Boolean),
    ottr:Triple(ex:anObject, ex:hasVal, ?UInt32),
    ottr:Triple(ex:anObject, ex:hasVal, ?UInt64),
    ottr:Triple(ex:anObject, ex:hasVal, ?Int32),
    ottr:Triple(ex:anObject, ex:hasVal, ?Int64),
    ottr:Triple(ex:anotherObject, ex:hasValVal, ?Float32),
    ottr:Triple(ex:anotherObject, ex:hasValVal, ?Float64),
    ottr:Triple(ex:yetAnotherObject, ex:hasString, ?Utf8),
    ottr:Triple(ex:yetAnotherObject, ex:hasDateTime, ?Datetime_ms_tz),
    ottr:Triple(ex:yetAnotherObject, ex:hasDateTime, ?Datetime_ms)
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
    let datetime_ms_tz = Series::from_any_values(
        "Datetime_ms_tz",
        &[
            AnyValue::Datetime(
                1656842780123,
                TimeUnit::Milliseconds,
                &Some("Europe/Oslo".to_string()),
            ),
            AnyValue::Datetime(
                1656842781456,
                TimeUnit::Milliseconds,
                &Some("Europe/Oslo".to_string()),
            ),
        ],
    )
    .unwrap();
    let datetime_ms = Series::from_any_values(
        "Datetime_ms",
        &[
            AnyValue::Datetime(1656842790789, TimeUnit::Milliseconds, &None),
            AnyValue::Datetime(1656842791101, TimeUnit::Milliseconds, &None),
        ],
    )
    .unwrap();

    let series = [
        k,
        boolean,
        uint32,
        uint64,
        int32,
        int64,
        float32,
        float64,
        utf8,
        datetime_ms_tz,
        datetime_ms,
    ];
    let df = DataFrame::from_iter(series);
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            df,
            Default::default(),
        )
        .unwrap();
    let mut actual_triples = mapping.export_oxrdf_triples();
    let mut expected_triples = vec![
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
                "5",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedInt"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "6",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedInt"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "7",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedLong"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "8",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedLong"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "-13",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "-14",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "-15",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#long"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "-16",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#long"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#anotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasValVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "17.18",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#float"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#anotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasValVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "19.2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#float"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#anotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasValVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "21.22",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#double"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#anotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasValVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "23.24",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#double"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasString"),
            object: Term::Literal(Literal::new_typed_literal(
                "abcde",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasString"),
            object: Term::Literal(Literal::new_typed_literal(
                "fghij",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasDateTime"),
            object: Term::Literal(Literal::new_typed_literal(
                "2022-07-03T10:06:20.123+02:00",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#dateTimeStamp"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasDateTime"),
            object: Term::Literal(Literal::new_typed_literal(
                "2022-07-03T10:06:21.456+02:00",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#dateTimeStamp"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasDateTime"),
            object: Term::Literal(Literal::new_typed_literal(
                "2022-07-03T10:06:30.789",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#dateTime"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasDateTime"),
            object: Term::Literal(Literal::new_typed_literal(
                "2022-07-03T10:06:31.101",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#dateTime"),
            )),
        },
    ];
    expected_triples.sort_by_key(|x| x.to_string());
    actual_triples.sort_by_key(|x| x.to_string());

    assert_eq!(expected_triples, actual_triples);
}

#[rstest]
#[serial]
fn test_list_arguments() {
    let stottr = r#"
@prefix ex:<http://example.net/ns#>.
ex:AnotherExampleTemplate [?object, ?predicate, ?myList] :: {
    cross | ottr:Triple(?object, ?predicate, ++?myList)
  } .
"#;
    let mut mapping = Mapping::from_str(&stottr).unwrap();
    let mut k = Series::from_iter(["KeyOne", "KeyOne", "KeyTwo", "KeyTwo"]);
    k.rename("Key");
    let mut object = Series::from_iter([
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj2",
        "http://example.net/ns#obj2",
    ]);
    object.rename("object");
    let mut predicate = Series::from_iter([
        "http://example.net/ns#hasNumberFromList1",
        "http://example.net/ns#hasNumberFromList1",
        "http://example.net/ns#hasNumberFromList2",
        "http://example.net/ns#hasNumberFromList2",
    ]);
    predicate.rename("predicate");
    let mut my_list = Series::from_iter([1i32, 2, 3, 4]);
    my_list.rename("myList");
    let series = [k, object, predicate, my_list];
    let mut df = DataFrame::from_iter(series);
    df = df
        .groupby_stable(["Key", "object", "predicate"])
        .unwrap()
        .agg_list()
        .unwrap();
    df.rename("myList_agg_list", "myList").unwrap();
    //println!("{df}");
    let _report = mapping
        .expand(
            "http://example.net/ns#AnotherExampleTemplate",
            df,
            Default::default(),
        )
        .unwrap();
    let triples = mapping.export_oxrdf_triples();
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples.into_iter());
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumberFromList1"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumberFromList1"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumberFromList2"),
            object: Term::Literal(Literal::new_typed_literal(
                "3",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumberFromList2"),
            object: Term::Literal(Literal::new_typed_literal(
                "4",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_two_list_arguments() {
    let stottr = r#"
@prefix ex:<http://example.net/ns#>.
ex:AnotherExampleTemplate [?subject, ?myList1, ?myList2] :: {
    cross | ex:Nested(?subject, ++?myList1, ++?myList2)
  } .
  ex:Nested [?subject, ?myVar1, ?myVar2] :: {
    ottr:Triple(?subject, ex:hasNumber, ?myVar1),
    ottr:Triple(?subject, ex:hasOtherNumber, ?myVar2)
} .
"#;
    let mut mapping = Mapping::from_str(&stottr).unwrap();
    let mut k = Series::from_iter(["KeyOne", "KeyOne", "KeyTwo", "KeyTwo", "KeyTwo"]);
    k.rename("Key");
    let mut subject = Series::from_iter([
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj2",
        "http://example.net/ns#obj2",
        "http://example.net/ns#obj2",
    ]);
    subject.rename("subject");
    let mut my_list1 = Series::from_iter([Some(1), Some(2), Some(3), Some(4), None]);
    my_list1.rename("myList1");
    let mut my_list2 = Series::from_iter([5, 6, 7, 8, 9]);
    my_list2.rename("myList2");
    let series = [k, subject, my_list1, my_list2];
    let mut df = DataFrame::from_iter(series);
    df = df
        .groupby_stable(["Key", "subject"])
        .unwrap()
        .agg_list()
        .unwrap();
    df.rename("myList1_agg_list", "myList1").unwrap();
    df.rename("myList2_agg_list", "myList2").unwrap();

    //println!("{df}");
    let _report = mapping
        .expand(
            "http://example.net/ns#AnotherExampleTemplate",
            df,
            Default::default(),
        )
        .unwrap();
    let triples = mapping.export_oxrdf_triples();
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples.into_iter());
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "5",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "6",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "3",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "4",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "7",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "8",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "9",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}
