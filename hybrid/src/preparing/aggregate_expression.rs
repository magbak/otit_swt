use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::preparing::expressions::EXPrepReturn;
use oxrdf::Variable;
use spargebra::algebra::{AggregateExpression, GraphPattern};
use std::collections::HashSet;

pub struct AEReturn {
    pub aggregate_expression: Option<AggregateExpression>,
    pub graph_pattern_pushups: Vec<GraphPattern>,
}

impl AEReturn {
    fn new() -> AEReturn {
        AEReturn {
            aggregate_expression: None,
            graph_pattern_pushups: vec![],
        }
    }

    fn with_aggregate_expression(
        &mut self,
        aggregate_expression: AggregateExpression,
    ) -> &mut AEReturn {
        self.aggregate_expression = Some(aggregate_expression);
        self
    }

    fn with_pushups(&mut self, exr: &mut EXPrepReturn) -> &mut AEReturn {
        self.graph_pattern_pushups.extend(
            exr.graph_pattern_pushups
                .drain(0..exr.graph_pattern_pushups.len()),
        );
        self
    }
}

impl TimeSeriesQueryPrepper {
    pub fn prepare_aggregate_expression(
        &mut self,
        aggregate_expression: &AggregateExpression,
        try_groupby_complex_query:bool,
        context: &Context,
    ) -> AEReturn {
        let mut aer = AEReturn::new();
        match aggregate_expression {
            AggregateExpression::Count { expr, distinct } => {
            }
            AggregateExpression::Sum { expr, distinct } => {
                let mut expr_prepared = self.prepare_expression(
                    expr,
                    try_groupby_complex_query,
                    &context.extension_with(PathEntry::AggregationOperation),
                );
            }
            AggregateExpression::Avg { expr, distinct } => {
                let mut expr_prepared = self.prepare_expression(
                    expr,
                    try_groupby_complex_query,
                    &context.extension_with(PathEntry::AggregationOperation),
                );
            }
            AggregateExpression::Min { expr, distinct } => {
                let mut expr_prepared = self.prepare_expression(
                    expr,
                    try_groupby_complex_query,
                    &context.extension_with(PathEntry::AggregationOperation),
                );
            }
            AggregateExpression::Max { expr, distinct } => {
                let mut expr_prepared = self.prepare_expression(
                    expr,
                    try_groupby_complex_query,
                    &context.extension_with(PathEntry::AggregationOperation),
                );
            }
            AggregateExpression::GroupConcat {
                expr,
                distinct,
                separator,
            } => {
                let mut expr_prepared = self.prepare_expression(
                    expr,
                    try_groupby_complex_query,
                    &context.extension_with(PathEntry::AggregationOperation),
                );
            }
            AggregateExpression::Sample { expr, distinct } => {
                let mut expr_prepared = self.prepare_expression(
                    expr,
                    try_groupby_complex_query,
                    &context.extension_with(PathEntry::AggregationOperation),
                );
            }
            AggregateExpression::Custom {
                name,
                expr,
                distinct,
            } => {
                let mut expr_prepared = self.prepare_expression(
                    expr,
                    try_groupby_complex_query,
                    &context.extension_with(PathEntry::AggregationOperation),
                );
            }
        }
        aer
    }
}
