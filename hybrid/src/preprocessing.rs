use crate::constants::{HAS_DATA_POINT, HAS_TIMESERIES, HAS_TIMESTAMP, HAS_VALUE};
use crate::constraints::Constraint;
use crate::find_query_variables::{
    find_all_used_variables_in_aggregate_expression, find_all_used_variables_in_expression,
};
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression};
use spargebra::term::{BlankNode, NamedNodePattern, TermPattern, TriplePattern, Variable};
use spargebra::Query;
use std::collections::{HashMap, HashSet};

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
                let preprocessed_expression = if let Some(e) = expression {
                    Some(self.preprocess_expression(e))
                } else {
                    None
                };
                GraphPattern::LeftJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    expression: preprocessed_expression,
                }
            }
            GraphPattern::Filter { expr, inner } => {
                let inner = self.preprocess_graph_pattern(inner);
                GraphPattern::Filter {
                    inner: Box::new(inner),
                    expr: self.preprocess_expression(expr),
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
                let mut used_vars = HashSet::new();
                find_all_used_variables_in_expression(expression, &mut used_vars);
                for v in used_vars.drain() {
                    if let Some(ctr) = self.has_constraint.get(&v) {
                        if ctr == &Constraint::ExternalDataValue
                            || ctr == &Constraint::ExternalTimestamp
                            || ctr == &Constraint::ExternallyDerived
                        {
                            if !self.has_constraint.contains_key(variable) {
                                self.has_constraint
                                    .insert(variable.clone(), Constraint::ExternallyDerived);
                            }
                        }
                    }
                }

                GraphPattern::Extend {
                    inner: Box::new(inner),
                    variable: variable.clone(),
                    expression: self.preprocess_expression(expression),
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
                    expression: expression.iter().map(|oe| self.preprocess_order_expression(oe)).collect()
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
                for (variable, agg) in aggregates {
                    let mut used_vars = HashSet::new();
                    find_all_used_variables_in_aggregate_expression(agg, &mut used_vars);
                    for v in used_vars.drain() {
                        if let Some(ctr) = self.has_constraint.get(&v) {
                            if ctr == &Constraint::ExternalDataValue
                                || ctr == &Constraint::ExternalTimestamp
                                || ctr == &Constraint::ExternallyDerived
                            {
                                self.has_constraint
                                    .insert(variable.clone(), Constraint::ExternallyDerived);
                            }
                        }
                    }
                }
                let mut preprocessed_aggregates = vec![];
                for (var, agg) in aggregates {
                    preprocessed_aggregates
                        .push((var.clone(), self.preprocess_aggregate_expression(agg)))
                }
                GraphPattern::Group {
                    inner: Box::new(inner),
                    variables: variables.clone(),
                    aggregates: preprocessed_aggregates,
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

    fn preprocess_expression(&mut self, expression: &Expression) -> Expression {
        match expression {
            Expression::Or(left, right) => Expression::Or(
                Box::new(self.preprocess_expression(left)),
                Box::new(self.preprocess_expression(right)),
            ),
            Expression::And(left, right) => Expression::And(
                Box::new(self.preprocess_expression(left)),
                Box::new(self.preprocess_expression(right)),
            ),
            Expression::Not(inner) => Expression::Not(Box::new(self.preprocess_expression(inner))),
            Expression::Exists(graph_pattern) => {
                Expression::Exists(Box::new(self.preprocess_graph_pattern(graph_pattern)))
            }
            Expression::If(left, middle, right) => Expression::If(
                Box::new(self.preprocess_expression(left)),
                Box::new(self.preprocess_expression(middle)),
                Box::new(self.preprocess_expression(right)),
            ),
            _ => expression.clone(),
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
    fn preprocess_aggregate_expression(
        &mut self,
        aggregate_expression: &AggregateExpression,
    ) -> AggregateExpression {
        match aggregate_expression {
            AggregateExpression::Count { expr, distinct } => {
                let rewritten_expression = if let Some(e) = expr {
                    Some(Box::new(self.preprocess_expression(e)))
                } else {
                    None
                };
                AggregateExpression::Count {
                    expr: rewritten_expression,
                    distinct: *distinct,
                }
            }
            AggregateExpression::Sum { expr, distinct } => AggregateExpression::Sum {
                expr: Box::new(self.preprocess_expression(expr)),
                distinct: *distinct,
            },
            AggregateExpression::Avg { expr, distinct } => AggregateExpression::Avg {
                expr: Box::new(self.preprocess_expression(expr)),
                distinct: *distinct,
            },
            AggregateExpression::Min { expr, distinct } => AggregateExpression::Min {
                expr: Box::new(self.preprocess_expression(expr)),
                distinct: *distinct,
            },
            AggregateExpression::Max { expr, distinct } => AggregateExpression::Max {
                expr: Box::new(self.preprocess_expression(expr)),
                distinct: *distinct,
            },
            AggregateExpression::GroupConcat {
                expr,
                distinct,
                separator,
            } => AggregateExpression::GroupConcat {
                expr: Box::new(self.preprocess_expression(expr)),
                distinct: *distinct,
                separator: separator.clone(),
            },
            AggregateExpression::Sample { expr, distinct } => AggregateExpression::Sample {
                expr: Box::new(self.preprocess_expression(expr)),
                distinct: *distinct,
            },
            AggregateExpression::Custom {
                name,
                expr,
                distinct,
            } => AggregateExpression::Custom {
                name: name.clone(),
                expr: Box::new(self.preprocess_expression(expr)),
                distinct: *distinct,
            },
        }
    }
    fn preprocess_order_expression(&mut self, order_expression: &OrderExpression) -> OrderExpression {
        match order_expression {
            OrderExpression::Asc(e) => {OrderExpression::Asc(self.preprocess_expression(e))}
            OrderExpression::Desc(e) => {OrderExpression::Desc(self.preprocess_expression(e))}
        }
    }
}
