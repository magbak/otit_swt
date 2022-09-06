use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use oxrdf::Variable;
use spargebra::algebra::Expression;
use std::collections::HashSet;
use crate::preparing::expressions::EXPrepReturn;

impl TimeSeriesQueryPrepper {
    pub fn prepare_and_expression(
        &mut self,
        left: &Expression,
        right: &Expression,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> EXPrepReturn {
        // We allow translations of left- or right hand sides of And-expressions to be None.
        // This allows us to enforce the remaining conditions that were not removed due to a prepare
        let mut left_prepare = self.prepare_expression(
            left,
            try_groupby_complex_query,
            &context.extension_with(PathEntry::AndLeft),
        );
        let mut right_prepare = self.prepare_expression(
            right,
            try_groupby_complex_query,
            &context.extension_with(PathEntry::AndRight),
        );
    }
}
