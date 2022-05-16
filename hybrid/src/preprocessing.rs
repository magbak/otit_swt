use crate::const_uris::{HAS_DATA_POINT, HAS_TIMESERIES, HAS_TIMESTAMP, HAS_VALUE};
use crate::constraints::Constraint;
use spargebra::algebra::{GraphPattern, PropertyPathExpression};
use spargebra::term::{BlankNode, NamedNodePattern, TermPattern, TriplePattern, Variable};
use spargebra::Query;
use std::collections::HashMap;

pub struct Preprocessor {
    counter: u16,
    blank_node_rename: HashMap<BlankNode, Variable>,
    has_constraint: HashMap<Variable, Constraint>,
}

impl Preprocessor {
    pub fn new() -> Preprocessor {
        Preprocessor {
            counter: 0,
            blank_node_rename: Default::default(),
            has_constraint: Default::default(),
        }
    }

    pub fn preprocess(&mut self, select_query: &Query) -> (Query, HashMap<Variable, Constraint>) {
        if let Query::Select {
            dataset,
            pattern,
            base_iri,
        } = &select_query
        {
            let gp = self.preprocess_graph_pattern(&pattern);
            let map = self.has_constraint.clone();
            let new_query = Query::Select {
                dataset: dataset.clone(),
                pattern: gp,
                base_iri: base_iri.clone(),
            };
            (new_query, map)
        } else {
            panic!("Should only be called with Select")
        }
    }

    fn preprocess_graph_pattern(&mut self, graph_pattern: &GraphPattern) -> GraphPattern {
        match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                let mut new_patterns = vec![];
                for p in patterns {
                    new_patterns.push(self.preprocess_triple_pattern(p));
                }
                GraphPattern::Bgp {
                    patterns: new_patterns,
                }
            }
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.preprocess_path(subject, path, object),
            GraphPattern::Join { left, right } => {
                let left = self.preprocess_graph_pattern(left);
                let right = self.preprocess_graph_pattern(right);
                GraphPattern::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                let left = self.preprocess_graph_pattern(left);
                let right = self.preprocess_graph_pattern(right);
                GraphPattern::LeftJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    expression: expression.clone(),
                }
            }
            GraphPattern::Filter { expr, inner } => {
                let inner = self.preprocess_graph_pattern(inner);
                GraphPattern::Filter {
                    inner: Box::new(inner),
                    expr: expr.clone(),
                }
            }
            GraphPattern::Union { left, right } => {
                let left = self.preprocess_graph_pattern(left);
                let right = self.preprocess_graph_pattern(right);
                GraphPattern::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
            GraphPattern::Graph { name, inner } => {
                let inner = self.preprocess_graph_pattern(inner);
                GraphPattern::Graph {
                    inner: Box::new(inner),
                    name: name.clone(),
                }
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                let inner = self.preprocess_graph_pattern(inner);
                GraphPattern::Extend {
                    inner: Box::new(inner),
                    variable: variable.clone(),
                    expression: expression.clone(),
                }
            }
            GraphPattern::Minus { left, right } => {
                let left = self.preprocess_graph_pattern(left);
                let right = self.preprocess_graph_pattern(right);
                GraphPattern::Minus {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => GraphPattern::Values {
                variables: variables.clone(),
                bindings: bindings.clone(),
            },
            GraphPattern::OrderBy { inner, expression } => {
                let inner = self.preprocess_graph_pattern(inner);
                GraphPattern::OrderBy {
                    inner: Box::new(inner),
                    expression: expression.clone(),
                }
            }
            GraphPattern::Project { inner, variables } => {
                let inner = self.preprocess_graph_pattern(inner);
                GraphPattern::Project {
                    inner: Box::new(inner),
                    variables: variables.clone(),
                }
            }
            GraphPattern::Distinct { inner } => {
                let inner = self.preprocess_graph_pattern(inner);
                GraphPattern::Distinct {
                    inner: Box::new(inner),
                }
            }
            GraphPattern::Reduced { inner } => {
                let inner = self.preprocess_graph_pattern(inner);
                GraphPattern::Reduced {
                    inner: Box::new(inner),
                }
            }
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => {
                let inner = self.preprocess_graph_pattern(inner);
                GraphPattern::Slice {
                    inner: Box::new(inner),
                    start: start.clone(),
                    length: length.clone(),
                }
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                let inner = self.preprocess_graph_pattern(inner);
                GraphPattern::Group {
                    inner: Box::new(inner),
                    variables: variables.clone(),
                    aggregates: aggregates.clone(),
                }
            }
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => {
                let inner = self.preprocess_graph_pattern(inner);
                GraphPattern::Service {
                    inner: Box::new(inner),
                    name: name.clone(),
                    silent: silent.clone(),
                }
            }
        }
    }

    fn preprocess_triple_pattern(&mut self, triple_pattern: &TriplePattern) -> TriplePattern {
        let new_subject = self.rename_if_blank(&triple_pattern.subject);
        let new_object = self.rename_if_blank(&triple_pattern.object);
        if let NamedNodePattern::NamedNode(named_predicate_node) = &triple_pattern.predicate {
            if let (
                TermPattern::Variable(new_subject_variable),
                TermPattern::Variable(new_object_variable),
            ) = (&new_subject, &new_object)
            {
                if named_predicate_node == &HAS_TIMESERIES {
                    self.has_constraint
                        .insert(new_object_variable.clone(), Constraint::ExternalTimeseries);
                }
                if named_predicate_node == &HAS_TIMESTAMP {
                    self.has_constraint
                        .insert(new_object_variable.clone(), Constraint::ExternalTimestamp);
                    self.has_constraint
                        .insert(new_subject_variable.clone(), Constraint::ExternalDataPoint);
                }
                if named_predicate_node == &HAS_VALUE {
                    self.has_constraint
                        .insert(new_object_variable.clone(), Constraint::ExternalDataValue);
                    self.has_constraint
                        .insert(new_subject_variable.clone(), Constraint::ExternalDataPoint);
                }
                if named_predicate_node == &HAS_DATA_POINT {
                    self.has_constraint
                        .insert(new_object_variable.clone(), Constraint::ExternalDataPoint);
                    self.has_constraint
                        .insert(new_subject_variable.clone(), Constraint::ExternalTimeseries);
                }
            }
        }
        return TriplePattern {
            subject: new_subject,
            predicate: triple_pattern.predicate.clone(),
            object: new_object,
        };
    }

    fn rename_if_blank(&mut self, term_pattern: &TermPattern) -> TermPattern {
        if let TermPattern::BlankNode(bn) = term_pattern {
            if let Some(var) = self.blank_node_rename.get(bn) {
                TermPattern::Variable(var.clone())
            } else {
                let var =
                    Variable::new("blank_replacement_".to_string() + &self.counter.to_string())
                        .expect("Name is ok");
                self.counter += 1;
                self.blank_node_rename.insert(bn.clone(), var.clone());
                TermPattern::Variable(var)
            }
        } else {
            term_pattern.clone()
        }
    }

    fn preprocess_path(
        &mut self,
        subject: &TermPattern,
        path: &PropertyPathExpression,
        object: &TermPattern,
    ) -> GraphPattern {
        let new_subject = self.rename_if_blank(subject);
        let new_object = self.rename_if_blank(object);
        GraphPattern::Path {
            subject: new_subject,
            path: path.clone(),
            object: new_object,
        }
    }
}
