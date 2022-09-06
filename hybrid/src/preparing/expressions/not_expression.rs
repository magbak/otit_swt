use super::TimeSeriesQueryPrepper;
use crate::query_context::{Context, PathEntry};
use crate::preparing::expressions::EXPrepReturn;
use spargebra::algebra::Expression;

impl TimeSeriesQueryPrepper {
    pub fn prepare_not_expression(
        &mut self,
        wrapped: &Expression,
                try_groupby_complex_query: bool,
        context: &Context,
    ) -> EXPrepReturn {
        let mut wrapped_prepare = self.prepare_expression(
            wrapped,
            try_groupby_complex_query,
            &context.extension_with(PathEntry::Not),
        );
        wrapped_prepare
    }
}
