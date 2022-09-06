use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use oxrdf::Variable;
use spargebra::algebra::{Expression, Function};
use std::collections::HashSet;
use crate::preparing::expressions::EXPrepReturn;

impl TimeSeriesQueryPrepper {
    pub fn prepare_function_call_expression(
        &mut self,
        fun: &Function,
        args: &Vec<Expression>,
                try_groupby_complex_query: bool,
        context: &Context,
    ) -> EXPrepReturn {
        let mut args_prepared = args
            .iter()
            .enumerate()
            .map(|(i, e)| {
                self.prepare_expression(
                    e,
                    &context.extension_with(PathEntry::FunctionCall(i as u16)),
                )
            })
            .collect::<Vec<EXPrepReturn>>();
    }
}
