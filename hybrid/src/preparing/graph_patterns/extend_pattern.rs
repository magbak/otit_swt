use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::pushups::apply_pushups;
use oxrdf::Variable;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashSet;
use crate::find_query_variables::find_all_used_variables_in_expression;

impl TimeSeriesQueryPrepper {
    pub(crate) fn prepare_extend(
        &mut self,
        inner: &GraphPattern,
        var: &Variable,
        expr: &Expression,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut inner_prepare = self.prepare_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::ExtendInner),
        );
        if try_groupby_complex_query {
        let mut expression_vars = HashSet::new();
        find_all_used_variables_in_expression(expression, &mut expression_vars);
        for tsq in &inner_prepare.time_series_queries {
            let mut found_all = true;
            let mut found_some = false;
            for expression_var in &expression_vars {
                if tsq.has_equivalent_value_variable(expression_var, context) {
                    found_some = true;
                } else if tsq.has_equivalent_timeseries_variable(expression_var, context) {
                    found_some = true;
                } else {

                }
            }
        }
        }
        Ok(())
    }
}
