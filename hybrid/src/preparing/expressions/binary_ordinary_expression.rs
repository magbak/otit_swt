use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use oxrdf::Variable;
use spargebra::algebra::Expression;
use std::collections::HashSet;
use crate::preparing::expressions::EXPrepReturn;

pub enum BinaryOrdinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    LessOrEqual,
    Less,
    Greater,
    GreaterOrEqual,
    SameTerm,
    Equal,
}

impl TimeSeriesQueryPrepper {
    pub fn prepare_binary_ordinary_expression(
        &mut self,
        left: &Expression,
        right: &Expression,
        operation: &BinaryOrdinaryOperator,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> EXPrepReturn {
        let (left_path_entry, right_path_entry, binary_expression): (
            _,
            _,
            fn(Box<_>, Box<_>) -> Expression,
        ) = match { operation } {
            BinaryOrdinaryOperator::Add => {
                (PathEntry::AddLeft, PathEntry::AddRight, Expression::Add)
            }
            BinaryOrdinaryOperator::Subtract => (
                PathEntry::SubtractLeft,
                PathEntry::SubtractRight,
                Expression::Subtract,
            ),
            BinaryOrdinaryOperator::Multiply => (
                PathEntry::MultiplyLeft,
                PathEntry::MultiplyRight,
                Expression::Multiply,
            ),
            BinaryOrdinaryOperator::Divide => (
                PathEntry::DivideLeft,
                PathEntry::DivideRight,
                Expression::Divide,
            ),
            BinaryOrdinaryOperator::LessOrEqual => (
                PathEntry::LessOrEqualLeft,
                PathEntry::LessOrEqualRight,
                Expression::LessOrEqual,
            ),
            BinaryOrdinaryOperator::Less => {
                (PathEntry::LessLeft, PathEntry::LessRight, Expression::Less)
            }
            BinaryOrdinaryOperator::Greater => (
                PathEntry::GreaterLeft,
                PathEntry::GreaterRight,
                Expression::Greater,
            ),
            BinaryOrdinaryOperator::GreaterOrEqual => (
                PathEntry::GreaterOrEqualLeft,
                PathEntry::GreaterOrEqualRight,
                Expression::GreaterOrEqual,
            ),
            BinaryOrdinaryOperator::SameTerm => (
                PathEntry::SameTermLeft,
                PathEntry::SameTermRight,
                Expression::SameTerm,
            ),
            BinaryOrdinaryOperator::Equal => (
                PathEntry::EqualLeft,
                PathEntry::EqualRight,
                Expression::Equal,
            ),
        };

        let mut left_prepare = self.prepare_expression(
            left,
            &context.extension_with(left_path_entry),
        );
        let mut right_prepare = self.prepare_expression(
            right,
            &context.extension_with(right_path_entry),
        );
    }
}
