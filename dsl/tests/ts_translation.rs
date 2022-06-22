use dsl::ast::{Connective, ConnectiveType};
use dsl::connective_mapping::ConnectiveMapping;
use dsl::costants::{REPLACE_STR_LITERAL, REPLACE_VARIABLE_NAME};
use dsl::parser::ts_query;
use dsl::translator::Translator;
use log::debug;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{Literal, NamedNode, Variable};
use rstest::*;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::HashMap;

#[fixture]
fn use_logger() {
    let res = env_logger::try_init();
    match res {
        Ok(_) => {}
        Err(_) => {
            debug!("Tried to initialize logger which is already initialize")
        }
    }
}

#[fixture]
fn type_name_template() -> Vec<TriplePattern> {
    let type_variable = Variable::new_unchecked("type_var");
    let type_triple = TriplePattern {
        subject: TermPattern::Variable(Variable::new_unchecked(REPLACE_VARIABLE_NAME)),
        predicate: NamedNodePattern::NamedNode(NamedNode::from(rdf::TYPE)),
        object: TermPattern::Variable(type_variable.clone()),
    };
    let type_name_triple = TriplePattern {
        subject: TermPattern::Variable(type_variable),
        predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(
            "http://example.org/types#hasName",
        )),
        object: TermPattern::Literal(Literal::new_typed_literal(REPLACE_STR_LITERAL, xsd::STRING)),
    };
    vec![type_triple, type_name_triple]
}

#[fixture]
fn name_template() -> Vec<TriplePattern> {
    let name_triple = TriplePattern {
        subject: TermPattern::Variable(Variable::new_unchecked(REPLACE_VARIABLE_NAME)),
        predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(
            "http://example.org/types#hasName",
        )),
        object: TermPattern::Literal(Literal::new_typed_literal(REPLACE_STR_LITERAL, xsd::STRING)),
    };
    vec![name_triple]
}

#[fixture]
fn connective_mapping() -> ConnectiveMapping {
    let map = HashMap::from([
        (
            Connective::new(ConnectiveType::Period, 1).to_string(),
            "http://example.org/types#hasOnePeriodRelation".to_string(),
        ),
        (
            Connective::new(ConnectiveType::Period, 2).to_string(),
            "http://example.org/types#hasTwoPeriodRelation".to_string(),
        ),
        (
            Connective::new(ConnectiveType::Dash, 1).to_string(),
            "http://example.org/types#hasOneDashRelation".to_string(),
        ),
    ]);

    ConnectiveMapping { map }
}

#[fixture]
fn translator(
    name_template: Vec<TriplePattern>,
    type_name_template: Vec<TriplePattern>,
    connective_mapping: ConnectiveMapping,
) -> Translator<'static> {
    Translator::new(name_template, type_name_template, connective_mapping)
}

#[rstest]
fn test_easy_translation(mut translator: Translator) {
    let q = r#"
    ABC-[valve]"HLV"."Mvm"."stVal"
    [valve]."PosPct"."mag"
    from 2021-12-01T00:00:01+01:00
    to 2021-12-02T00:00:01+01:00
"#;
    let (_, tsq) = ts_query(q).expect("No problemo");
    let mut actual = translator.translate(&tsq);
    actual = Query::parse(&actual.to_string(), None).expect("Parse myself");

    let expected_query_str = r#"
  PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
  PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  SELECT ?valve__Dash___Mvm___Period___stVal__path_name ?valve_PosPct___Period___mag__path_name ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value ?valve_PosPct___Period___mag__timeseries_datapoint_value WHERE {
  ?ABC rdf:type ?type_var_0.
  ?type_var_0 <http://example.org/types#hasName> "ABC".
  ?valve <http://example.org/types#hasName> "HLV".
  ?ABC <http://example.org/types#hasOneDashRelation> ?valve.
  ?valve__Dash___Mvm_ <http://example.org/types#hasName> "Mvm".
  ?valve <http://example.org/types#hasOnePeriodRelation> ?valve__Dash___Mvm_.
  ?valve__Dash___Mvm___Period___stVal_ <http://example.org/types#hasName> "stVal".
  ?valve__Dash___Mvm_ <http://example.org/types#hasOnePeriodRelation> ?valve__Dash___Mvm___Period___stVal_.
  ?valve__Dash___Mvm___Period___stVal_ <https://github.com/magbak/quarry-rs#hasTimeseries> ?valve__Dash___Mvm___Period___stVal__timeseries.
  ?valve__Dash___Mvm___Period___stVal__timeseries <https://github.com/magbak/quarry-rs#hasTimeseries> ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint.
  ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint <https://github.com/magbak/quarry-rs#hasValue> ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value.
  ?valve__Dash___Mvm___Period___stVal__timeseries <https://github.com/magbak/quarry-rs#hasTimestamp> ?timestamp.
  ?valve <http://example.org/types#hasName> ?valve_name_on_path.
  ?valve__Dash___Mvm_ <http://example.org/types#hasName> ?valve__Dash___Mvm__name_on_path.
  ?valve__Dash___Mvm___Period___stVal_ <http://example.org/types#hasName> ?valve__Dash___Mvm___Period___stVal__name_on_path.
  ?valve_PosPct_ <http://example.org/types#hasName> "PosPct".
  ?valve <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct_.
  ?valve_PosPct___Period___mag_ <http://example.org/types#hasName> "mag".
  ?valve_PosPct_ <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct___Period___mag_.
  ?valve_PosPct___Period___mag_ <https://github.com/magbak/quarry-rs#hasTimeseries> ?valve_PosPct___Period___mag__timeseries.
  ?valve_PosPct___Period___mag__timeseries <https://github.com/magbak/quarry-rs#hasTimeseries> ?valve_PosPct___Period___mag__timeseries_datapoint.
  ?valve_PosPct___Period___mag__timeseries_datapoint <https://github.com/magbak/quarry-rs#hasValue> ?valve_PosPct___Period___mag__timeseries_datapoint_value.
  ?valve_PosPct___Period___mag__timeseries <https://github.com/magbak/quarry-rs#hasTimestamp> ?timestamp.
  ?valve_PosPct_ <http://example.org/types#hasName> ?valve_PosPct__name_on_path.
  ?valve_PosPct___Period___mag_ <http://example.org/types#hasName> ?valve_PosPct___Period___mag__name_on_path.
  FILTER(("2021-11-30T23:00:01+00:00"^^xsd:dateTime >= ?timestamp) && ("2021-12-01T23:00:01+00:00"^^xsd:dateTime <= ?timestamp))
  BIND(CONCAT(?valve_name_on_path, "-", ?valve__Dash___Mvm__name_on_path, ".", ?valve__Dash___Mvm___Period___stVal__name_on_path, ".") AS ?valve__Dash___Mvm___Period___stVal__path_name)
  BIND(CONCAT(?valve_PosPct__name_on_path, ".", ?valve_PosPct___Period___mag__name_on_path, ".") AS ?valve_PosPct___Period___mag__path_name)
}"#;
    let expected_query = Query::parse(expected_query_str, None).expect("Parse expected error");
    assert_eq!(expected_query, actual);
}
