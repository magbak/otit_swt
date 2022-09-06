use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::preparing::aggregate_expression::AEReturn;
use crate::preparing::pushups::apply_pushups;
use oxrdf::Variable;
use spargebra::algebra::{AggregateExpression, GraphPattern};
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::pushdown_setting::PushdownSetting;
use crate::timeseries_query::{GroupedTimeSeriesQuery, TimeSeriesQuery};

impl TimeSeriesQueryPrepper {
    pub fn prepare_group(
        &mut self,
        graph_pattern: &GraphPattern,
        by: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            return GPPrepReturn::fail()
        }
        let mut try_graph_pattern_prepare = self.prepare_graph_pattern(
            graph_pattern,
            true,
            &context.extension_with(PathEntry::GroupInner),
        );

        if !try_graph_pattern_prepare.fail_groupby_complex_query && self.pushdown_settings.contains(&PushdownSetting::GroupBy){
            let mut time_series_queries = try_graph_pattern_prepare.drain_time_series_queries();

            if time_series_queries.len() == 1 {
                let mut tsq = time_series_queries.remove(0);
                add_basic_groupby_mapping_values(&mut tsq, &self.static_result_df, by, aggregates);
                GroupedTimeSeriesQuery {
                    tsq: Box::new(tsq),
                    graph_pattern_context: context.clone(),
                    by: vec![],
                    aggregations: vec![],
                    timeseries_funcs: vec![]
                };
            }
        }

        self.prepare_graph_pattern(
        graph_pattern,
        false,
        &context.extension_with(PathEntry::GroupInner))
    }
}
