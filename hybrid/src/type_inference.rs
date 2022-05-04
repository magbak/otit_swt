use polars::export::arrow::compute::arithmetics::sub;
use spargebra::algebra::{GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNode, NamedNodePattern, TermPattern, TriplePattern};
use std::collections::BTreeMap;
use crate::const_uris::{HAS_DATA_POINT, HAS_TIMESERIES, HAS_TIMESTAMP, HAS_VALUE};
use crate::constraints::Constraint;

pub fn infer_types(&graph_pattern: GraphPattern) -> BTreeMap<TermPattern, Constraint> {
    let mut tree = BTreeMap::new();
    infer_types_rec(&graph_pattern, &mut tree);
    tree
}

pub fn infer_graph_pattern(
    graph_pattern: &GraphPattern,
    tree: &mut BTreeMap<TermPattern, Constraint>,
) {
    match graph_pattern {
        GraphPattern::Bgp { patterns } => {
            for p in patterns {
                infer_triple_pattern(p, tree)
            }
        }
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            infer_property_path(subject, path, object, tree);
        }
        GraphPattern::Join { left, right } => {
            infer_graph_pattern(left, tree);
            infer_graph_pattern(right, tree);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            infer_graph_pattern(left, tree);
            infer_graph_pattern(right, tree);
        }
        GraphPattern::Filter { expr, inner } => {
            infer_graph_pattern(inner, tree);
        }
        GraphPattern::Union { left, right } => {
            infer_graph_pattern(left, tree);
            infer_graph_pattern(right, tree);
        }
        GraphPattern::Graph { name, inner } => {
            infer_graph_pattern(inner, tree);
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            infer_graph_pattern(inner, tree);
        }
        GraphPattern::Minus { left, right } => {
            infer_graph_pattern(left, tree);
            infer_graph_pattern(right, tree);
        }
        GraphPattern::Values {
            variables,
            bindings,
        } => {
            //No action
        }
        GraphPattern::OrderBy { inner, expression } => {
            infer_graph_pattern(inner, tree);
        }
        GraphPattern::Project { inner, variables } => {
            infer_graph_pattern(inner, tree);
        }
        GraphPattern::Distinct { inner } => {
            infer_graph_pattern(inner, tree);
        }
        GraphPattern::Reduced { inner } => {
            infer_graph_pattern(inner, tree);
        }
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            infer_graph_pattern(inner, tree);
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            infer_graph_pattern(inner, tree);
        }
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => {
            infer_graph_pattern(inner, tree);
        }
    }
}

pub fn infer_property_path(
    subject: &TermPattern,
    property_path: &PropertyPathExpression,
    object: &TermPattern,
    tree: &BTreeMap<TermPattern, Constraint>,
) {
    //We only support type inference for one type of property path
    if let PropertyPathExpression::Sequence(p1, p2) = property_path {
        let last = get_last_elem(p1);
        if let PropertyPathExpression::NamedNode(n1) = last {
            if let PropertyPathExpression::NamedNode(n2) = p2 {
                if n1 == HAS_TIMESERIES && n2 == HAS_DATA_POINT {
                    todo!();
                }
                if n1 == HAS_DATA_POINT && n2 == HAS_VALUE {
                    todo!();
                }
                if n1 == HAS_DATA_POINT && n2 == HAS_TIMESTAMP {
                    todo!();
                }
            }
        }
    }
}

fn get_last_elem(p: &PropertyPathExpression) -> &PropertyPathExpression {
    let PropertyPathExpression::Sequence(_, last) = p;
    if let PropertyPathExpression::Sequence(_, _) = last {
        get_last_elem(last)
    } else {
        last
    }
}

pub fn infer_triple_pattern(
    triple_pattern: &TriplePattern,
    tree: &mut BTreeMap<TermPattern, Constraint>,
) {
    match triple_pattern {
        TriplePattern {
            subject,
            predicate,
            object,
        } => {
            if let Some(NamedNodePattern::NamedNode(named_predicate_node)) = predicate {
                if named_predicate_node == HAS_TIMESERIES {
                    tree.insert(object.clone(), Constraint::ExternalTimeseries);
                }
                if named_predicate_node == HAS_TIMESTAMP {
                    tree.insert(object.clone(), Constraint::ExternalTimestamp);
                    tree.insert(subject.clone(), Constraint::ExternalDataPoint);
                }
                if named_predicate_node == HAS_VALUE {
                    tree.insert(object.clone(), Constraint::ExternalDataValue);
                    tree.insert(subject.clone(), Constraint::ExternalDataPoint);
                }
                if named_predicate_node == HAS_DATA_POINT {
                    tree.insert(object.clone(), Constraint::ExternalDataPoint);
                    tree.insert(subject.clone(), Constraint::ExternalTimeseries);
                }
            }
        }
    }
}
