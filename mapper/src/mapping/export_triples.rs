use super::Mapping;
use crate::mapping::is_blank_node;
use oxrdf::{BlankNode, Literal, NamedNode, Subject, Term, Triple};
use polars_core::prelude::AnyValue;

impl Mapping {
    pub fn object_property_triples<F, T>(&self, func: F, out: &mut Vec<T>)
    where
        F: Fn(&str, &str, &str) -> T,
    {
        if let Some(object_property_triples) = &self.object_property_triples {
            if object_property_triples.height() > 0 {
                let mut subject_iterator =
                    object_property_triples.column("subject").unwrap().iter();
                let mut verb_iterator = object_property_triples.column("verb").unwrap().iter();
                let mut object_iterator = object_property_triples.column("object").unwrap().iter();
                for _ in 0..object_property_triples.height() {
                    let s = anyutf8_to_str(subject_iterator.next().unwrap());
                    let v = anyutf8_to_str(verb_iterator.next().unwrap());
                    let o = anyutf8_to_str(object_iterator.next().unwrap());
                    out.push(func(s, v, o));
                }
            }
        } else {
            panic!("")
        }
    }

    pub fn data_property_triples<F, T>(&self, func: F, out: &mut Vec<T>)
    where
        F: Fn(&str, &str, &str, Option<&str>, &str) -> T,
    {
        //subject, verb, lexical_form, language_tag, datatype
        if let Some(data_property_triples) = &self.data_property_triples {
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
                    let s = anyutf8_to_str(subject_iterator.next().unwrap());
                    let v = anyutf8_to_str(verb_iterator.next().unwrap());
                    let lex = anyutf8_to_str(lexical_iterator.next().unwrap());
                    let lang = anyutf8_to_str(language_tag_iterator.next().unwrap());
                    let lang_opt = if lang == "" { None } else { Some(lang) };
                    let dt = anyutf8_to_str(datatype_iterator.next().unwrap());
                    out.push(func(s, v, lex, lang_opt, dt));
                }
            }
        } else {
            panic!("")
        }
    }

    pub fn export_oxrdf_triples(&self) -> Vec<Triple> {
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

        fn object_triple_func(s: &str, v: &str, o: &str) -> Triple {
            let subject = subject_from_str(s);
            let verb = NamedNode::new_unchecked(v);
            let object = object_term_from_str(o);
            Triple::new(subject, verb, object)
        }

        fn data_triple_func(
            s: &str,
            v: &str,
            lex: &str,
            lang_opt: Option<&str>,
            dt: &str,
        ) -> Triple {
            let subject = subject_from_str(s);
            let verb = NamedNode::new_unchecked(v);
            let literal = if let Some(lang) = lang_opt {
                Literal::new_language_tagged_literal_unchecked(lex, lang)
            } else {
                Literal::new_typed_literal(lex, NamedNode::new_unchecked(dt))
            };
            Triple::new(subject, verb, Term::Literal(literal))
        }

        let mut triples = vec![];
        self.object_property_triples(object_triple_func, &mut triples);
        self.data_property_triples(data_triple_func, &mut triples);
        triples
    }
}
fn anyutf8_to_str(a: AnyValue) -> &str {
    if let AnyValue::Utf8(s) = a {
        s
    } else {
        panic!("Should never happen")
    }
}
