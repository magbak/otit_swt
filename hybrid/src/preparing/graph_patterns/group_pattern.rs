use std::collections::HashSet;
use log::debug;
use crate::query_context::{Context, PathEntry};

use oxrdf::Variable;
use polars_core::frame::DataFrame;
use spargebra::algebra::{AggregateExpression, GraphPattern};
use crate::constants::GROUPING_COL;
use crate::find_query_variables::find_all_used_variables_in_aggregate_expression;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::pushdown_setting::PushdownSetting;
use crate::timeseries_query::{GroupedTimeSeriesQuery, TimeSeriesQuery};
use super::TimeSeriesQueryPrepper;

impl TimeSeriesQueryPrepper<'_> {
    pub fn prepare_group(
        &mut self,
        graph_pattern: &GraphPattern,
        by: &Vec<Variable>,
        aggregations: &Vec<(Variable, AggregateExpression)>,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            return GPPrepReturn::fail_groupby_complex_query()
        }
        let inner_context = &context.extension_with(PathEntry::GroupInner);
        let mut try_graph_pattern_prepare = self.prepare_graph_pattern(
            graph_pattern,
            true,
            &inner_context,
        );

        if !try_graph_pattern_prepare.fail_groupby_complex_query && self.pushdown_settings.contains(&PushdownSetting::GroupBy){
            let mut time_series_queries = try_graph_pattern_prepare.drained_time_series_queries();

            if time_series_queries.len() == 1 {
                let mut tsq = time_series_queries.remove(0);
                let in_scope = check_aggregations_are_in_scope(&tsq, inner_context, aggregations);

                if in_scope {
                    let grouping_col = self.add_grouping_col(by);
                    tsq = add_basic_groupby_mapping_values(tsq, &self.static_result_df, &grouping_col);
                    GroupedTimeSeriesQuery {
                        tsq: Box::new(tsq),
                        graph_pattern_context: context.clone(),
                        aggregations: aggregations.clone(),
                    };
                }
            }
        }

        self.prepare_graph_pattern(
        graph_pattern,
        false,
        &context.extension_with(PathEntry::GroupInner))
    }
    fn add_grouping_col(&self, by: &Vec<Variable>) -> String {
        todo!()
    }
}

fn check_aggregations_are_in_scope(tsq: &TimeSeriesQuery, context:&Context, aggregations: &Vec<(Variable, AggregateExpression)>) -> bool {
    for (_, ae) in aggregations {
        let mut used_vars = HashSet::new();
        find_all_used_variables_in_aggregate_expression(ae, &mut used_vars);
        for v in &used_vars {
            if tsq.has_equivalent_timestamp_variable(v, context) {
                continue
            } else if tsq.has_equivalent_value_variable(v, context) {
                continue
            } else {
                debug!("Variable {:?} in aggregate expression not in scope", v);
                return false;
            }
        }
    }
    true
}

fn add_basic_groupby_mapping_values(tsq:TimeSeriesQuery, static_result_df:&DataFrame, grouping_col:&str) -> TimeSeriesQuery{
    match tsq {
        TimeSeriesQuery::Basic(b) => {
            let mut by_vec = vec![grouping_col, b.identifier_variable.as_ref().unwrap().as_str()];
            let df = static_result_df.select(by_vec).unwrap();
            TimeSeriesQuery::GroupedBasic(
                b,
                df,
                    grouping_col.to_string()
            )
        }
        TimeSeriesQuery::Filtered(tsq, f) => {
            TimeSeriesQuery::Filtered(Box::new(add_basic_groupby_mapping_values(*tsq, static_result_df, grouping_col)), f)
        }
        TimeSeriesQuery::InnerSynchronized(inners, syncs) => {
            let mut tsq_added = vec![];
            for tsq in inners {
               tsq_added.push(Box::new(add_basic_groupby_mapping_values(*tsq, static_result_df, grouping_col)))
            }
            TimeSeriesQuery::InnerSynchronized(tsq_added, syncs)
        }
        TimeSeriesQuery::ExpressionAs(tsq, v, e) => {
            TimeSeriesQuery::ExpressionAs(Box::new(add_basic_groupby_mapping_values(*tsq, static_result_df, grouping_col)), v,e)
        }
        TimeSeriesQuery::Grouped(_) => {
            panic!("Should never happen")
        }
        TimeSeriesQuery::GroupedBasic(_,_,_) => {
            panic!("Should never happen")
        }
    }
}

