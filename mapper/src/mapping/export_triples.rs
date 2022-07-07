use crate::mapping::{is_blank_node};
use oxrdf::{BlankNode, Literal, NamedNode, Subject, Term, Triple};
use polars_core::frame::DataFrame;
use polars_core::prelude::AnyValue;

pub fn export_triples(
    object_property_triples: &Option<DataFrame>,
    data_property_triples: &Option<DataFrame>,
) -> Vec<Triple> {
    let mut triples = vec![];
    if let Some(object_property_triples) = object_property_triples {
        if object_property_triples.height() > 0 {
            let mut subject_iterator = object_property_triples.column("subject").unwrap().iter();
            let mut verb_iterator = object_property_triples.column("verb").unwrap().iter();
            let mut object_iterator = object_property_triples.column("object").unwrap().iter();

            for _ in 0..object_property_triples.height() {
                let s = subject_iterator.next().unwrap();
                let v = verb_iterator.next().unwrap();
                let o = object_iterator.next().unwrap();

                if let AnyValue::Utf8(subject) = s {
                    if let AnyValue::Utf8(verb) = v {
                        if let AnyValue::Utf8(object) = o {
                            let subject = subject_from_str(subject);
                            let verb = NamedNode::new_unchecked(verb);
                            let object = object_term_from_str(object);
                            let t = Triple::new(subject, verb, object);
                            triples.push(t);
                        } else {
                            panic!("Should never happen")
                        }
                    } else {
                        panic!("Should also never happen")
                    }
                } else {
                    panic!("Also never")
                }
            }
        }
    }

    if let Some(data_property_triples) = data_property_triples {
        if data_property_triples.height() > 0 {
            let mut subject_iterator = data_property_triples.column("subject").unwrap().iter();
            let mut verb_iterator = data_property_triples.column("verb").unwrap().iter();
            //Workaround due to not happy about struct iterator..
            let obj_col = data_property_triples.column("object").unwrap();
            let lexical_form_series = obj_col
                .struct_()
                .unwrap()
                .field_by_name("lexical_form")
                .unwrap();
            let language_tag_series = obj_col
                .struct_()
                .unwrap()
                .field_by_name("language_tag")
                .unwrap();
            let datatype_series = obj_col
                .struct_()
                .unwrap()
                .field_by_name("datatype_iri")
                .unwrap();

            let mut lexical_iterator = lexical_form_series.iter();
            let mut language_tag_iterator = language_tag_series.iter();
            let mut datatype_iterator = datatype_series.iter();

            for _ in 0..data_property_triples.height() {
                let s = subject_iterator.next().unwrap();
                let v = verb_iterator.next().unwrap();
                let l = lexical_iterator.next().unwrap();
                let t = language_tag_iterator.next().unwrap();
                let d = datatype_iterator.next().unwrap();

                //TODO: Fix for when subject might be blank node.
                if let AnyValue::Utf8(subject) = s {
                    if let AnyValue::Utf8(verb) = v {
                        if let AnyValue::Utf8(value) = l {
                            if let AnyValue::Utf8(tag) = t {
                                if let AnyValue::Utf8(datatype) = d {
                                    let subject = subject_from_str(subject);
                                    let verb = NamedNode::new_unchecked(verb);
                                    let object;
                                    if tag != "" {
                                        object = Term::Literal(
                                            Literal::new_language_tagged_literal_unchecked(
                                                value, tag,
                                            ),
                                        )
                                    } else {
                                        object = Term::Literal(Literal::new_typed_literal(
                                            value,
                                            NamedNode::new_unchecked(datatype),
                                        ));
                                    }
                                    let t = Triple::new(subject, verb, object);
                                    triples.push(t);
                                } else {
                                    panic!("Should never happen")
                                }
                            } else {
                                panic!("Should never happen")
                            }
                        } else {
                            panic!("Should never happen")
                        }
                    } else {
                        panic!("Should never happen")
                    }
                } else {
                    panic!("Should also never happen")
                }
            }
        }
    }
    triples
}

fn subject_from_str(s: &str) -> Subject {
    if is_blank_node(s) {
        Subject::BlankNode(BlankNode::new_unchecked(s))
    } else {
        Subject::NamedNode(NamedNode::new_unchecked(s))
    }
}

fn object_term_from_str(s: &str) -> Term {
    if is_blank_node(s) {
        Term::BlankNode(BlankNode::new_unchecked(s))
    } else {
        Term::NamedNode(NamedNode::new_unchecked(s))
    }
}
