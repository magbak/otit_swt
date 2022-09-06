use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::preparing::expressions::EXPrepReturn;
use oxrdf::Variable;
use spargebra::algebra::Expression;
use std::collections::HashSet;

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
        let (path_entry, expression): (_, fn(Box<Expression>) -> Expression) = match operation {
            UnaryOrdinaryOperator::UnaryPlus => (PathEntry::UnaryPlus, Expression::UnaryPlus),
            UnaryOrdinaryOperator::UnaryMinus => (PathEntry::UnaryMinus, Expression::UnaryMinus),
        };
        let mut wrapped_prepare = self.prepare_expression(
            wrapped,
            try_groupby_complex_query,
            &context.extension_with(path_entry),
        );
    }
}
