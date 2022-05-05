use polars::export::arrow::compute::arithmetics::sub;
use spargebra::algebra::{GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNode, NamedNodePattern, TermPattern, TriplePattern};
use std::collections::HashMap;
use std::ops::Deref;
use spargebra::Query;
use crate::const_uris::{HAS_DATA_POINT, HAS_TIMESERIES, HAS_TIMESTAMP, HAS_VALUE};
use crate::constraints::Constraint;

pub fn infer_types(select_query:&Query) -> HashMap<TermPattern, Constraint> {
    let Query::Select { dataset, pattern, base_iri } = &select_query;
    let mut has_constraint = HashMap::new();
    infer_graph_pattern(&pattern, &mut has_constraint);
    has_constraint
}

pub fn infer_graph_pattern(
    graph_pattern: &GraphPattern,
    has_constraint: &mut HashMap<TermPattern, Constraint>,
) {
    match graph_pattern {
        GraphPattern::Bgp { patterns } => {
            for p in patterns {
                infer_triple_pattern(p, has_constraint)
            }
        }
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            infer_property_path(subject, path, object, has_constraint);
        }
        GraphPattern::Join { left, right } => {
            infer_graph_pattern(left, has_constraint);
            infer_graph_pattern(right, has_constraint);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            infer_graph_pattern(left, has_constraint);
            infer_graph_pattern(right, has_constraint);
        }
        GraphPattern::Filter { expr, inner } => {
            infer_graph_pattern(inner, has_constraint);
        }
        GraphPattern::Union { left, right } => {
            infer_graph_pattern(left, has_constraint);
            infer_graph_pattern(right, has_constraint);
        }
        GraphPattern::Graph { name, inner } => {
            infer_graph_pattern(inner, has_constraint);
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            infer_graph_pattern(inner, has_constraint);
        }
        GraphPattern::Minus { left, right } => {
            infer_graph_pattern(left, has_constraint);
            infer_graph_pattern(right, has_constraint);
        }
        GraphPattern::Values {
            variables,
            bindings,
        } => {
            //No action
        }
        GraphPattern::OrderBy { inner, expression } => {
            infer_graph_pattern(inner, has_constraint);
        }
        GraphPattern::Project { inner, variables } => {
            infer_graph_pattern(inner, has_constraint);
        }
        GraphPattern::Distinct { inner } => {
            infer_graph_pattern(inner, has_constraint);
        }
        GraphPattern::Reduced { inner } => {
            infer_graph_pattern(inner, has_constraint);
        }
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            infer_graph_pattern(inner, has_constraint);
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            infer_graph_pattern(inner, has_constraint);
        }
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => {
            infer_graph_pattern(inner, has_constraint);
        }
    }
}

pub fn infer_property_path(
    subject: &TermPattern,
    property_path: &PropertyPathExpression,
    object: &TermPattern,
    has_constraint: &HashMap<TermPattern, Constraint>,
) {
    //We only support type inference for one type of property path
    if let PropertyPathExpression::Sequence(p1, p2) = property_path {
        let last = get_last_elem(p1);
        if let PropertyPathExpression::NamedNode(n1) = last.deref() {
            if let PropertyPathExpression::NamedNode(n2) = p2.deref() {
                if n1 == &HAS_TIMESERIES && n2 == &HAS_DATA_POINT {
                    todo!();
                }
                if n1 == &HAS_DATA_POINT && n2 == &HAS_VALUE {
                    todo!();
                }
                if n1 == &HAS_DATA_POINT && n2 == &HAS_TIMESTAMP {
                    todo!();
                }
            }
        }
    }
}

fn get_last_elem(p: &PropertyPathExpression) -> &PropertyPathExpression {
    let PropertyPathExpression::Sequence(_, last) = p;
    if let PropertyPathExpression::Sequence(_, _) = last.deref() {
        get_last_elem(last)
    } else {
        last
    }
}

pub fn infer_triple_pattern(
    triple_pattern: &TriplePattern,
    has_constraint: &mut HashMap<TermPattern, Constraint>,
) {
    match triple_pattern {
        TriplePattern {
            subject,
            predicate,
            object,
        } => {
            if let NamedNodePattern::NamedNode(named_predicate_node) = predicate {
                if named_predicate_node == &HAS_TIMESERIES {
                    has_constraint.insert(object.clone(), Constraint::ExternalTimeseries);
                }
                if named_predicate_node == &HAS_TIMESTAMP {
                    has_constraint.insert(object.clone(), Constraint::ExternalTimestamp);
                    has_constraint.insert(subject.clone(), Constraint::ExternalDataPoint);
                }
                if named_predicate_node == &HAS_VALUE {
                    has_constraint.insert(object.clone(), Constraint::ExternalDataValue);
                    has_constraint.insert(subject.clone(), Constraint::ExternalDataPoint);
                }
                if named_predicate_node == &HAS_DATA_POINT {
                    has_constraint.insert(object.clone(), Constraint::ExternalDataPoint);
                    has_constraint.insert(subject.clone(), Constraint::ExternalTimeseries);
                }
            }
        }
    }
}
