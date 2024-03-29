mod and_expression;
mod binary_ordinary_expression;
mod coalesce_expression;
mod exists_expression;
mod function_call_expression;
mod if_expression;
mod in_expression;
mod not_expression;
mod or_expression;
mod unary_ordinary_expression;

use super::TimeSeriesQueryPrepper;
use crate::preparing::expressions::binary_ordinary_expression::BinaryOrdinaryOperator;
use crate::preparing::expressions::unary_ordinary_expression::UnaryOrdinaryOperator;
use crate::query_context::Context;
use crate::timeseries_query::TimeSeriesQuery;
use spargebra::algebra::Expression;

pub struct EXPrepReturn {
    pub fail_groupby_complex_query: bool,
    pub time_series_queries: Vec<TimeSeriesQuery>,
}

impl EXPrepReturn {
    fn new(time_series_queries: Vec<TimeSeriesQuery>) -> EXPrepReturn {
        EXPrepReturn {
            time_series_queries,
            fail_groupby_complex_query: false,
        }
    }

    pub fn fail_groupby_complex_query() -> EXPrepReturn {
        EXPrepReturn {
            fail_groupby_complex_query: true,
            time_series_queries: vec![],
        }
    }

    pub fn with_time_series_queries_from(&mut self, other: &mut EXPrepReturn) {
        self.time_series_queries
            .extend(other.drained_time_series_queries());
    }

    pub fn drained_time_series_queries(&mut self) -> Vec<TimeSeriesQuery> {
        self.time_series_queries
            .drain(0..self.time_series_queries.len())
            .collect()
    }
}

impl TimeSeriesQueryPrepper {
    pub fn prepare_expression(
        &mut self,
        expression: &Expression,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> EXPrepReturn {
        match expression {
            Expression::NamedNode(..) => {
                let exr = EXPrepReturn::new(vec![]);
                exr
            }
            Expression::Literal(..) => {
                let exr = EXPrepReturn::new(vec![]);
                exr
            }
            Expression::Variable(..) => EXPrepReturn::new(vec![]),
            Expression::Or(left, right) => {
                self.prepare_or_expression(left, right, try_groupby_complex_query, context)
            }

            Expression::And(left, right) => {
                self.prepare_and_expression(left, right, try_groupby_complex_query, context)
            }
            Expression::Equal(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Equal,
                try_groupby_complex_query,
                context,
            ),
            Expression::SameTerm(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::SameTerm,
                try_groupby_complex_query,
                context,
            ),
            Expression::Greater(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Greater,
                try_groupby_complex_query,
                context,
            ),
            Expression::GreaterOrEqual(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::GreaterOrEqual,
                try_groupby_complex_query,
                context,
            ),
            Expression::Less(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Less,
                try_groupby_complex_query,
                context,
            ),
            Expression::LessOrEqual(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::LessOrEqual,
                try_groupby_complex_query,
                context,
            ),
            Expression::In(left, expressions) => {
                self.prepare_in_expression(left, expressions, try_groupby_complex_query, context)
            }
            Expression::Add(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Add,
                try_groupby_complex_query,
                context,
            ),
            Expression::Subtract(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Subtract,
                try_groupby_complex_query,
                context,
            ),
            Expression::Multiply(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Multiply,
                try_groupby_complex_query,
                context,
            ),
            Expression::Divide(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Divide,
                try_groupby_complex_query,
                context,
            ),
            Expression::UnaryPlus(wrapped) => self.prepare_unary_ordinary_expression(
                wrapped,
                &UnaryOrdinaryOperator::UnaryPlus,
                try_groupby_complex_query,
                context,
            ),
            Expression::UnaryMinus(wrapped) => self.prepare_unary_ordinary_expression(
                wrapped,
                &UnaryOrdinaryOperator::UnaryMinus,
                try_groupby_complex_query,
                context,
            ),
            Expression::Not(wrapped) => {
                self.prepare_not_expression(wrapped, try_groupby_complex_query, context)
            }
            Expression::Exists(wrapped) => {
                self.prepare_exists_expression(wrapped, try_groupby_complex_query, context)
            }
            Expression::Bound(..) => EXPrepReturn::new(vec![]),
            Expression::If(left, mid, right) => {
                self.prepare_if_expression(left, mid, right, try_groupby_complex_query, context)
            }
            Expression::Coalesce(wrapped) => {
                self.prepare_coalesce_expression(wrapped, try_groupby_complex_query, context)
            }
            Expression::FunctionCall(fun, args) => {
                self.prepare_function_call_expression(fun, args, try_groupby_complex_query, context)
            }
        }
    }
}
