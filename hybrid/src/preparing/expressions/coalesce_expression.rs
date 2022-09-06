use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::preparing::expressions::EXPrepReturn;
use oxrdf::Variable;
use spargebra::algebra::Expression;
use std::collections::HashSet;

impl TimeSeriesQueryPrepper {
    pub fn prepare_coalesce_expression(
        &mut self,
        wrapped: &Vec<Expression>,
                try_groupby_complex_query: bool,
        context: &Context,
    ) -> EXPrepReturn {
        let mut prepared = wrapped
            .iter()
            .enumerate()
            .map(|(i, e)| {
                self.prepare_expression(
                    e,
                    try_groupby_complex_query,
                    &context.extension_with(PathEntry::Coalesce(i as u16)),
                )
            })
            .collect::<Vec<EXPrepReturn>>();
    }
}
