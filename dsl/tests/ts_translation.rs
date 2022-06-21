use std::collections::HashMap;
use log::debug;
use oxrdf::{Literal, NamedNode, Variable};
use oxrdf::vocab::{rdf, xsd};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use dsl::parser::ts_query;
use dsl::translator::Translator;
use rstest::*;
use dsl::ast::{Connective, ConnectiveType};
use dsl::connective_mapping::ConnectiveMapping;
use dsl::costants::{REPLACE_STR_LITERAL, REPLACE_VARIABLE_NAME};

#[fixture]
fn use_logger() {
    let res = env_logger::try_init();
    match res {
        Ok(_) => {}
        Err(_) => {debug!("Tried to initialize logger which is already initialize")}
    }
}

#[fixture]
fn type_name_template() -> Vec<TriplePattern> {
    let type_variable = Variable::new_unchecked("type_var");
    let type_triple = TriplePattern {
        subject: TermPattern::Variable(Variable::new_unchecked(REPLACE_VARIABLE_NAME)),
        predicate: NamedNodePattern::NamedNode(NamedNode::from(rdf::TYPE)),
        object: TermPattern::Variable(type_variable.clone())
    };
    let type_name_triple = TriplePattern {
        subject: TermPattern::Variable(type_variable),
        predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked("http://example.org/types#hasName")),
        object: TermPattern::Literal(Literal::new_typed_literal(REPLACE_STR_LITERAL, xsd::STRING))
    };
    vec![type_triple, type_name_triple]
}

#[fixture]
fn name_template() -> Vec<TriplePattern> {
    let name_triple = TriplePattern {
        subject: TermPattern::Variable(Variable::new_unchecked(REPLACE_VARIABLE_NAME)),
        predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked("http://example.org/types#hasName")),
        object: TermPattern::Literal(Literal::new_typed_literal(REPLACE_STR_LITERAL, xsd::STRING))
    };
    vec![name_triple]
}

#[fixture]
fn connective_mapping() -> ConnectiveMapping {
    let map = HashMap::from([
        (Connective::new(ConnectiveType::Period, 1).to_string(), "http://example.org/types#hasOnePeriodRelation".to_string()),
        (Connective::new(ConnectiveType::Period, 2).to_string(), "http://example.org/types#hasTwoPeriodRelation".to_string()),
        (Connective::new(ConnectiveType::Dash, 1).to_string(), "http://example.org/types#hasOneDashRelation".to_string())

    ]);

    ConnectiveMapping {map}
}

#[fixture]
fn translator(name_template:Vec<TriplePattern>, type_name_template:Vec<TriplePattern>, connective_mapping:ConnectiveMapping) -> Translator<'static> {
    Translator::new(name_template, type_name_template, connective_mapping)
}

#[rstest]
fn test_easy_translation(mut translator:Translator) {
        let q = r#"
    ABC-[valve]"HLV"."Mvm"."stVal"
    [valve]."PosPct"."mag"
    group valve
    from 2021-12-01T00:00:01+01:00
    to 2021-12-02T00:00:01+01:00
    aggregate mean 10min
"#;
    let (_, actual) = ts_query(q).expect("No problemo");
    let q = translator.translate(&actual);
    println!("{}", q);
}