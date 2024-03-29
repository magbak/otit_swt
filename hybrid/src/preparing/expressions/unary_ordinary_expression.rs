use super::TimeSeriesQueryPrepper;
use crate::preparing::expressions::EXPrepReturn;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;

pub enum UnaryOrdinaryOperator {
    UnaryPlus,
    UnaryMinus,
}

impl TimeSeriesQueryPrepper {
    pub fn prepare_unary_ordinary_expression(
        &mut self,
        wrapped: &Expression,
        operation: &UnaryOrdinaryOperator,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> EXPrepReturn {
        let path_entry = match operation {
            UnaryOrdinaryOperator::UnaryPlus => PathEntry::UnaryPlus,
            UnaryOrdinaryOperator::UnaryMinus => PathEntry::UnaryMinus,
        };
        let wrapped_prepare = self.prepare_expression(
            wrapped,
            try_groupby_complex_query,
            &context.extension_with(path_entry),
        );
        wrapped_prepare
    }
}
