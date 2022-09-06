use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::{Expression, GraphPattern};
use crate::preparing::expressions::EXPrepReturn;

impl TimeSeriesQueryPrepper {
    pub fn prepare_exists_expression(
        &mut self,
        wrapped: &GraphPattern,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> EXPrepReturn {
        let mut wrapped_prepare = self.prepare_graph_pattern(
            wrapped,
            try_groupby_complex_query
            &context.extension_with(PathEntry::Exists),
        );
    }
}
