use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::preparing::expressions::EXPrepReturn;
use oxrdf::Variable;
use spargebra::algebra::{GraphPattern, OrderExpression};
use std::collections::HashSet;

pub struct OEReturn {
    pub order_expression: Option<OrderExpression>,
    pub graph_pattern_pushups: Vec<GraphPattern>,
}

impl OEReturn {
    fn new() -> OEReturn {
        OEReturn {
            order_expression: None,
            graph_pattern_pushups: vec![],
        }
    }

    fn with_order_expression(&mut self, order_expression: OrderExpression) -> &mut OEReturn {
        self.order_expression = Some(order_expression);
        self
    }

    fn with_pushups(&mut self, exr: &mut EXPrepReturn) -> &mut OEReturn {
        self.graph_pattern_pushups.extend(
            exr.graph_pattern_pushups
                .drain(0..exr.graph_pattern_pushups.len()),
        );
        self
    }
}

impl TimeSeriesQueryPrepper {
    pub fn prepare_order_expression(
        &mut self,
        order_expression: &OrderExpression,
        variables_in_scope: &HashSet<Variable>,
        try_groupby_complex_query:bool,
        context: &Context,
    ) -> OEReturn {
        let mut oer = OEReturn::new();
        match order_expression {
            OrderExpression::Asc(e) => {
                let mut e_prepare = self.prepare_expression(
                    e,
                    try_groupby_complex_query,
                    &context.extension_with(PathEntry::OrderingOperation),
                );
            }
            OrderExpression::Desc(e) => {
                let mut e_prepare = self.prepare_expression(
                    e,
                    try_groupby_complex_query,
                    &context.extension_with(PathEntry::OrderingOperation),
                );
            }
        }
        oer
    }
}
