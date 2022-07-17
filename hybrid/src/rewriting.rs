mod aggregate_expression;
mod expressions;
mod graph_patterns;
mod order_expression;
mod project_static;
mod pushups;

use crate::change_types::ChangeType;
use crate::constants::{HAS_DATATYPE, HAS_DATA_POINT, HAS_TIMESTAMP, HAS_VALUE};
use crate::constraints::{Constraint, VariableConstraints};
use crate::query_context::PathEntry::ExtendExpression;
use crate::query_context::{Context, PathEntry, VariableInContext};
use crate::rewriting::expressions::ExReturn;
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::pushups::apply_pushups;
use crate::timeseries_query::TimeSeriesQuery;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};
use spargebra::Query;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub struct StaticQueryRewriter {
    variable_counter: u16,
    additional_projections: HashSet<Variable>,
    variable_constraints: VariableConstraints,
    pub time_series_queries: Vec<TimeSeriesQuery>,
}

impl StaticQueryRewriter {
    pub fn new(variable_constraints: &VariableConstraints) -> StaticQueryRewriter {
        StaticQueryRewriter {
            variable_counter: 0,
            additional_projections: Default::default(),
            variable_constraints: variable_constraints.clone(),
            time_series_queries: vec![],
        }
    }

    pub fn rewrite_query(&mut self, query: Query) -> Option<(Query, Vec<TimeSeriesQuery>)> {
        if let Query::Select {
            dataset,
            pattern,
            base_iri,
        } = &query
        {
            let required_change_direction = ChangeType::Relaxed;
            let pattern_rewrite_opt =
                self.rewrite_graph_pattern(pattern, &required_change_direction, &Context::new());
            if let Some(mut gpr_inner) = pattern_rewrite_opt {
                if &gpr_inner.change_type == &ChangeType::NoChange
                    || &gpr_inner.change_type == &ChangeType::Relaxed
                {
                    return Some((
                        Query::Select {
                            dataset: dataset.clone(),
                            pattern: gpr_inner.graph_pattern.take().unwrap(),
                            base_iri: base_iri.clone(),
                        },
                        self.time_series_queries.clone(),
                    ));
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            panic!("Only support for Select");
        }
    }

    fn project_all_static_variables(&mut self, rewrites: Vec<&ExReturn>, context: &Context) {
        for r in rewrites {
            if let Some(expr) = &r.expression {
                self.project_all_static_variables_in_expression(expr, context);
            }
        }
    }

    fn rewrite_extend(
        &mut self,
        inner: &GraphPattern,
        var: &Variable,
        expr: &Expression,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> Option<GPReturn> {
        let inner_rewrite_opt = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::ExtendInner),
        );
        if let Some(mut gpr_inner) = inner_rewrite_opt {
            let mut expr_rewrite = self.rewrite_expression(
                expr,
                &ChangeType::NoChange,
                &gpr_inner.variables_in_scope,
                &context.extension_with(PathEntry::ExtendExpression),
            );
            if expr_rewrite.expression.is_some() {
                gpr_inner.variables_in_scope.insert(var.clone());
                let inner_graph_pattern = gpr_inner.graph_pattern.take().unwrap();
                gpr_inner.with_graph_pattern(GraphPattern::Extend {
                    inner: Box::new(inner_graph_pattern), //No need for push up since there should be no change
                    variable: var.clone(),
                    expression: expr_rewrite.expression.take().unwrap(),
                });
                return Some(gpr_inner);
            } else {
                let inner_graph_pattern = gpr_inner.graph_pattern.take().unwrap();
                gpr_inner.with_graph_pattern(apply_pushups(
                    inner_graph_pattern,
                    &mut expr_rewrite.graph_pattern_pushups,
                ));
                return Some(gpr_inner);
            }
        }
        let expr_rewrite = self.rewrite_expression(
            expr,
            &ChangeType::NoChange,
            &HashSet::new(),
            &context.extension_with(ExtendExpression),
        );
        if expr_rewrite.graph_pattern_pushups.len() > 0 {
            todo!("Solution will require graph pattern pushups for graph patterns!!");
        }
        return None;
    }

    fn rewrite_variable(&self, v: &Variable, context: &Context) -> Option<Variable> {
        if let Some(ctr) = self.variable_constraints.get_constraint(v, context) {
            if !(ctr == &Constraint::ExternalDataPoint
                || ctr == &Constraint::ExternalDataValue
                || ctr == &Constraint::ExternalTimestamp
                || ctr == &Constraint::ExternallyDerived)
            {
                Some(v.clone())
            } else {
                None
            }
        } else {
            Some(v.clone())
        }
    }

    fn pushdown_expression(&mut self, expr: &Expression, context: &Context) {
        for t in &mut self.time_series_queries {
            t.try_rewrite_expression(expr, context);
        }
    }

    fn process_dynamic_triples(&mut self, dynamic_triples: Vec<&TriplePattern>, context: &Context) {
        for t in &dynamic_triples {
            if let NamedNodePattern::NamedNode(named_predicate_node) = &t.predicate {
                if named_predicate_node == HAS_DATA_POINT {
                    for q in &mut self.time_series_queries {
                        if let (
                            Some(q_timeseries_variable),
                            TermPattern::Variable(subject_variable),
                        ) = (&q.timeseries_variable, &t.subject)
                        {
                            if q_timeseries_variable.partial(subject_variable, context) {
                                if let TermPattern::Variable(ts_var) = &t.object {
                                    q.data_point_variable = Some(VariableInContext::new(
                                        ts_var.clone(),
                                        context.clone(),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        for t in &dynamic_triples {
            if let NamedNodePattern::NamedNode(named_predicate_node) = &t.predicate {
                if named_predicate_node == HAS_VALUE {
                    for q in &mut self.time_series_queries {
                        if q.value_variable.is_none() {
                            if let (
                                Some(q_data_point_variable),
                                TermPattern::Variable(subject_variable),
                            ) = (&q.data_point_variable, &t.subject)
                            {
                                if q_data_point_variable.partial(subject_variable, context) {
                                    if let TermPattern::Variable(value_var) = &t.object {
                                        q.value_variable = Some(VariableInContext::new(
                                            value_var.clone(),
                                            context.clone(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                } else if named_predicate_node == HAS_DATATYPE {
                    for q in &mut self.time_series_queries {
                        if q.datatype_variable.is_none() {
                            if let (
                                Some(q_data_point_variable),
                                TermPattern::Variable(subject_variable),
                            ) = (&q.data_point_variable, &t.subject)
                            {
                                if q_data_point_variable.partial(subject_variable, context) {
                                    if let TermPattern::Variable(datatype_var) = &t.object {
                                        q.datatype_variable = Some(VariableInContext::new(
                                            datatype_var.clone(),
                                            context.clone(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                } else if named_predicate_node == HAS_TIMESTAMP {
                    for q in &mut self.time_series_queries {
                        if q.timestamp_variable.is_none() {
                            if let (
                                Some(q_data_point_variable),
                                TermPattern::Variable(subject_variable),
                            ) = (&q.data_point_variable, &t.subject)
                            {
                                if q_data_point_variable.partial(subject_variable, context) {
                                    if let TermPattern::Variable(timestamp_var) = &t.object {
                                        q.timestamp_variable = Some(VariableInContext::new(
                                            timestamp_var.clone(),
                                            context.clone(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn create_time_series_query(
        &mut self,
        time_series_variable: &Variable,
        time_series_id_variable: &Variable,
        context: &Context,
    ) {
        let mut ts_query = TimeSeriesQuery::new();
        ts_query.identifier_variable = Some(time_series_id_variable.clone());
        ts_query.timeseries_variable = Some(VariableInContext::new(
            time_series_variable.clone(),
            context.clone(),
        ));
        self.time_series_queries.push(ts_query);
    }
}

pub(crate) fn hash_graph_pattern(graph_pattern: &GraphPattern) -> u64 {
    let mut hasher = DefaultHasher::new();
    graph_pattern.hash(&mut hasher);
    hasher.finish()
}
