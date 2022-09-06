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
use crate::change_types::ChangeType;
use crate::query_context::Context;
use oxrdf::Variable;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashSet;
use crate::preparing::expressions::binary_ordinary_expression::BinaryOrdinaryOperator;
use crate::preparing::expressions::unary_ordinary_expression::UnaryOrdinaryOperator;

pub struct EXPrepReturn {}

impl EXPrepReturn {
    fn new() -> EXPrepReturn {
        EXPrepReturn {
            
        }
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
            Expression::NamedNode(nn) => {
                let mut exr = EXPrepReturn::new();
                exr
            }
            Expression::Literal(l) => {
                let mut exr = EXPrepReturn::new();
                exr
            }
            Expression::Variable(v) => {
                if let Some(prepared_variable) = self.prepare_variable(v, context) {
                    if variables_in_scope.contains(v) {
                        let mut exr = EXPrepReturn::new();
                        exr.with_expression(Expression::Variable(prepared_variable))
                            .with_change_type(ChangeType::NoChange);
                        return exr;
                    }
                }
                EXPrepReturn::new()
            }
            Expression::Or(left, right) => self.prepare_or_expression(
                left,
                right,
                try_groupby_complex_query,
                context,
            ),

            Expression::And(left, right) => self.prepare_and_expression(
                left,
                right,
                try_groupby_complex_query,
                context,
            ),
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
            Expression::In(left, expressions) => self.prepare_in_expression(
                left,
                expressions,
                try_groupby_complex_query,
                context,
            ),
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
            Expression::Not(wrapped) => self.prepare_not_expression(
                wrapped,
                try_groupby_complex_query,
                context,
            ),
            Expression::Exists(wrapped) => self.prepare_exists_expression(wrapped, try_groupby_complex_query, context),
            Expression::Bound(v) => {
                let mut exr = EXPrepReturn::new();
                if let Some(v_prepared) = self.prepare_variable(v, context) {
                    exr.with_expression(Expression::Bound(v_prepared))
                        .with_change_type(ChangeType::NoChange);
                }
                exr
            }
            Expression::If(left, mid, right) => {
                self.prepare_if_expression(left, mid, right,try_groupby_complex_query, context)
            }
            Expression::Coalesce(wrapped) => {
                self.prepare_coalesce_expression(wrapped,  try_groupby_complex_query,context)
            }
            Expression::FunctionCall(fun, args) => {
                self.prepare_function_call_expression(fun, args, try_groupby_complex_query,context)
            }
        }
    }
}
