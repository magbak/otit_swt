use super::TimeSeriesTable;
use crate::timeseries_database::timeseries_sql_rewrite::{Name, TimeSeriesQueryToSQLError};
use sea_query::{Function, SimpleExpr};
use spargebra::algebra::AggregateExpression;
use std::collections::HashMap;

impl TimeSeriesTable {
    //TODO: Support distinct in aggregates.. how???
    pub(crate) fn sparql_aggregate_expression_to_sql_expression(
        &self,
        agg: &AggregateExpression,
        variable_column_name_map: &HashMap<String, String>,
        table_name: Option<&Name>,
    ) -> Result<SimpleExpr, TimeSeriesQueryToSQLError> {
        Ok(match agg {
            AggregateExpression::Count { expr, distinct: _ } => {
                if let Some(some_expr) = expr {
                    SimpleExpr::FunctionCall(
                        Function::Count,
                        vec![self.sparql_expression_to_sql_expression(
                            some_expr,
                            &variable_column_name_map,
                            table_name,
                        )?],
                    )
                } else {
                    todo!("")
                }
            }
            AggregateExpression::Sum { expr, distinct: _ } => SimpleExpr::FunctionCall(
                Function::Sum,
                vec![self.sparql_expression_to_sql_expression(
                    expr,
                    &variable_column_name_map,
                    table_name,
                )?],
            ),
            AggregateExpression::Avg { expr, distinct: _ } => SimpleExpr::FunctionCall(
                Function::Avg,
                vec![self.sparql_expression_to_sql_expression(
                    expr,
                    &variable_column_name_map,
                    table_name,
                )?],
            ),
            AggregateExpression::Min { expr, distinct: _ } => SimpleExpr::FunctionCall(
                Function::Min,
                vec![self.sparql_expression_to_sql_expression(
                    expr,
                    &variable_column_name_map,
                    table_name,
                )?],
            ),
            AggregateExpression::Max { expr, distinct: _ } => SimpleExpr::FunctionCall(
                Function::Max,
                vec![self.sparql_expression_to_sql_expression(
                    expr,
                    &variable_column_name_map,
                    table_name,
                )?],
            ),
            AggregateExpression::GroupConcat {
                expr: _,
                distinct: _,
                separator: _,
            } => {
                todo!("")
            }
            AggregateExpression::Sample {
                expr: _,
                distinct: _,
            } => {
                todo!("")
            }
            AggregateExpression::Custom {
                expr: _,
                distinct: _,
                name: _,
            } => {
                todo!("")
            }
        })
    }
}
