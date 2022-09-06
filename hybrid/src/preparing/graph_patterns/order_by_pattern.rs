use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::order_expression::OEReturn;
use crate::preparing::pushups::apply_pushups;
use spargebra::algebra::{GraphPattern, OrderExpression};

impl TimeSeriesQueryPrepper {
    pub fn prepare_order_by(
        &mut self,
        inner: &GraphPattern,
        order_expressions: &Vec<OrderExpression>,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut inner_prepare = self.prepare_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::OrderByInner),
        );
    }
}
