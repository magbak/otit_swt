use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use oxrdf::Variable;
use spargebra::algebra::Expression;
use std::collections::HashSet;
use crate::preparing::expressions::EXPrepReturn;

impl TimeSeriesQueryPrepper {
    pub fn prepare_in_expression(
        &mut self,
        left: &Expression,
        expressions: &Vec<Expression>,
        try_groupby_complex_query:bool,
        context: &Context,
    ) -> EXPrepReturn {
        let mut left_prepare = self.prepare_expression(
            left,
            try_groupby_complex_query,
            &context.extension_with(PathEntry::InLeft),
        );
    }
}
