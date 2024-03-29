use super::TimeSeriesQueryPrepper;
use crate::preparing::expressions::EXPrepReturn;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;

impl TimeSeriesQueryPrepper {
    pub fn prepare_if_expression(
        &mut self,
        left: &Expression,
        mid: &Expression,
        right: &Expression,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> EXPrepReturn {
        let mut left_prepare = self.prepare_expression(
            left,
            try_groupby_complex_query,
            &context.extension_with(PathEntry::IfLeft),
        );
        let mut mid_prepare = self.prepare_expression(
            mid,
            try_groupby_complex_query,
            &context.extension_with(PathEntry::IfMiddle),
        );
        let mut right_prepare = self.prepare_expression(
            right,
            try_groupby_complex_query,
            &context.extension_with(PathEntry::IfRight),
        );
        left_prepare.with_time_series_queries_from(&mut mid_prepare);
        left_prepare.with_time_series_queries_from(&mut right_prepare);
        left_prepare
    }
}
