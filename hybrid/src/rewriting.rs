mod aggregate_expression;
mod expressions;
mod graph_patterns;
mod order_expression;
mod project_static;
mod pushups;

use crate::change_types::ChangeType;
use crate::constraints::{Constraint, VariableConstraints};
use crate::pushdown_setting::PushdownSetting;
use crate::query_context::{Context, VariableInContext};
use crate::rewriting::expressions::ExReturn;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesQuery};
use spargebra::algebra::{GraphPattern};
use spargebra::term::Variable;
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
    pushdown_settings: HashSet<PushdownSetting>,
    allow_compound_timeseries_queries: bool,
}

impl StaticQueryRewriter {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        variable_constraints: &VariableConstraints,
        allow_compound_timeseries_queries: bool,
    ) -> StaticQueryRewriter {
        StaticQueryRewriter {
            allow_compound_timeseries_queries,
            pushdown_settings,
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
            let mut pattern_rewrite =
                self.rewrite_graph_pattern(pattern, &required_change_direction, &Context::new());
            if pattern_rewrite.graph_pattern.is_some() {
                if &pattern_rewrite.change_type == &ChangeType::NoChange
                    || &pattern_rewrite.change_type == &ChangeType::Relaxed
                {
                    return Some((
                        Query::Select {
                            dataset: dataset.clone(),
                            pattern: pattern_rewrite.graph_pattern.take().unwrap(),
                            base_iri: base_iri.clone(),
                        },
                        pattern_rewrite.drained_time_series_queries(),
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

    fn create_basic_time_series_query(
        &mut self,
        time_series_variable: &Variable,
        time_series_id_variable: &Variable,
        datatype_variable: &Variable,
        context: &Context,
    ) -> BasicTimeSeriesQuery {
        let mut ts_query = BasicTimeSeriesQuery::new_empty();
        ts_query.identifier_variable = Some(time_series_id_variable.clone());
        ts_query.datatype_variable = Some(datatype_variable.clone());
        ts_query.timeseries_variable = Some(VariableInContext::new(
            time_series_variable.clone(),
            context.clone(),
        ));
        ts_query
    }
}

pub(crate) fn hash_graph_pattern(graph_pattern: &GraphPattern) -> u64 {
    let mut hasher = DefaultHasher::new();
    graph_pattern.hash(&mut hasher);
    hasher.finish()
}
