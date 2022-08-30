mod aggregate_expressions;
mod expression_rewrite;
mod partitioning_support;

use crate::timeseries_query::TimeSeriesQuery;
use log::debug;
use oxrdf::{NamedNode};
use sea_query::{Alias, ColumnRef, PostgresQueryBuilder, Query, SimpleExpr};
use sea_query::{Expr as SeaExpr, Iden, Value};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter, Write};
use std::rc::Rc;

#[derive(Debug)]
pub enum TimeSeriesQueryToSQLError {
    UnknownVariable(String),
    UnknownDatatype(String),
    FoundNonValueInInExpression,
}

impl Display for TimeSeriesQueryToSQLError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeSeriesQueryToSQLError::UnknownVariable(v) => {
                write!(f, "Unknown variable {}", v)
            }
            TimeSeriesQueryToSQLError::UnknownDatatype(d) => {
                write!(f, "Unknown datatype: {}", d)
            }
            TimeSeriesQueryToSQLError::FoundNonValueInInExpression => {
                write!(f, "In-expression contained non-literal alternative")
            }
        }
    }
}

impl Error for TimeSeriesQueryToSQLError {}

#[derive(Clone)]
pub struct TimeSeriesTable {
    pub schema: Option<String>,
    pub time_series_table: String,
    pub value_column: String,
    pub timestamp_column: String,
    pub identifier_column: String,
    pub value_datatype: NamedNode,
    pub year_column: Option<String>,
    pub month_column: Option<String>,
    pub day_column: Option<String>,
}

impl TimeSeriesTable {
    pub fn create_query(&self, tsq: &TimeSeriesQuery) -> Result<String, TimeSeriesQueryToSQLError> {
        let mut inner_query = Query::select();
        let mut variable_column_name_map = HashMap::new();
        variable_column_name_map.insert(
            tsq.identifier_variable
                .as_ref()
                .unwrap()
                .as_str()
                .to_string(),
            self.identifier_column.clone(),
        );
        variable_column_name_map.insert(
            tsq.value_variable
                .as_ref()
                .unwrap()
                .variable
                .as_str()
                .to_string(),
            self.value_column.clone(),
        );
        variable_column_name_map.insert(
            tsq.timestamp_variable
                .as_ref()
                .unwrap()
                .variable
                .as_str()
                .to_string(),
            self.timestamp_column.clone(),
        );

        let mut kvs: Vec<_> = variable_column_name_map.iter().collect();
        kvs.sort();
        for (k, v) in kvs {
            inner_query.expr_as(SeaExpr::col(Name::Column(v.clone())), Alias::new(k));
        }
        if let Some(schema) = &self.schema {
            inner_query.from((
                Name::Schema(schema.clone()),
                Name::Table(self.time_series_table.clone()),
            ));
        } else {
            inner_query.from(Name::Table(self.time_series_table.clone()));
        }

        if let Some(ids) = &tsq.ids {
            inner_query.and_where(
                SeaExpr::col(Name::Column(self.identifier_column.clone())).is_in(
                    ids.iter()
                        .map(|x| Value::String(Some(Box::new(x.to_string())))),
                ),
            );
        }

        for c in &tsq.conditions {
            let mut se = self.sparql_expression_to_sql_expression(
                &c.expression,
                &variable_column_name_map,
                None,
            )?;
            if self.year_column.is_some()
                && self.month_column.is_some()
                && self.day_column.is_some()
            {
                se = self.add_partitioned_timestamp_conditions(se);
            }
            inner_query.and_where(se);
        }
        if let Some(grouping) = &tsq.grouping {
            let inner_query_str = "inner_query";
            let inner_query_name = Name::Table(inner_query_str.to_string());
            for (v, e) in grouping.timeseries_funcs.iter().rev() {
                let mut outer_query = Query::select();

                outer_query.from_subquery(inner_query, Alias::new(inner_query_str));
                outer_query.expr_as(
                    self.sparql_expression_to_sql_expression(
                        &e.expression,
                        &variable_column_name_map,
                        Some(&inner_query_name),
                    )?,
                    Alias::new(v.as_str()),
                );
                let mut kvs: Vec<_> = variable_column_name_map.iter().collect();
                kvs.sort();
                for (varname, _) in kvs {
                    outer_query.expr_as(
                        SimpleExpr::Column(ColumnRef::TableColumn(
                            Rc::new(inner_query_name.clone()),
                            Rc::new(Name::Column(varname.clone())),
                        )),
                        Alias::new(varname),
                    );
                }
                variable_column_name_map.insert(v.as_str().to_string(), v.as_str().to_string());
                inner_query = outer_query;
            }
            let mut outer_query = Query::select();
            outer_query.from_subquery(inner_query, Alias::new(inner_query_str));

            for (v, agg) in &grouping.aggregations {
                outer_query.expr_as(
                    self.sparql_aggregate_expression_to_sql_expression(
                        &agg.aggregate_expression,
                        &variable_column_name_map,
                        Some(&inner_query_name),
                    )?,
                    Alias::new(v.as_str()),
                );
            }

            outer_query.group_by_columns(
                grouping
                    .by
                    .iter()
                    .map(|x| {
                        ColumnRef::TableColumn(
                            Rc::new(inner_query_name.clone()),
                            Rc::new(Name::Column(x.as_str().to_string())),
                        )
                    })
                    .collect::<Vec<ColumnRef>>(),
            );
            for v in &grouping.by {
                outer_query.expr_as(
                    SimpleExpr::Column(ColumnRef::TableColumn(
                        Rc::new(inner_query_name.clone()),
                        Rc::new(Name::Column(v.as_str().to_string())),
                    )),
                    Alias::new(v.as_str()),
                );
            }
            inner_query = outer_query;
        }
        let query_string = inner_query.to_string(PostgresQueryBuilder);
        debug!("Query string: {}", query_string);
        Ok(query_string)
    }
}

#[derive(Clone)]
pub(crate) enum Name {
    Schema(String),
    Table(String),
    Column(String),
    Function(String),
}

impl Iden for Name {
    fn unquoted(&self, s: &mut dyn Write) {
        write!(
            s,
            "{}",
            match self {
                Name::Schema(s) => {
                    s
                }
                Name::Table(s) => {
                    s
                }
                Name::Column(s) => {
                    s
                }
                Name::Function(s) => {
                    s
                }
            }
        )
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::pushdown_setting::all_pushdowns;
    use crate::query_context::{Context, ExpressionInContext, VariableInContext};
    use crate::timeseries_database::timeseries_sql_rewrite::TimeSeriesTable;
    use crate::timeseries_query::TimeSeriesQuery;
    use oxrdf::vocab::xsd;
    use oxrdf::{Literal, NamedNode, Variable};
    use spargebra::algebra::Expression;

    #[test]
    pub fn test_translate() {
        let tsq = TimeSeriesQuery {
            pushdown_settings: all_pushdowns(),
            dropped_value_expression: false,
            identifier_variable: Some(Variable::new_unchecked("id")),
            timeseries_variable: Some(VariableInContext::new(
                Variable::new_unchecked("ts"),
                Context::new(),
            )),
            data_point_variable: Some(VariableInContext::new(
                Variable::new_unchecked("dp"),
                Context::new(),
            )),
            value_variable: Some(VariableInContext::new(
                Variable::new_unchecked("v"),
                Context::new(),
            )),
            datatype_variable: Some(Variable::new_unchecked("dt")),
            datatype: Some(xsd::INT.into_owned()),
            timestamp_variable: Some(VariableInContext::new(
                Variable::new_unchecked("t"),
                Context::new(),
            )),
            ids: Some(vec!["A".to_string(), "B".to_string()]),
            grouping: None,
            conditions: vec![ExpressionInContext {
                expression: Expression::LessOrEqual(
                    Box::new(Expression::Variable(Variable::new_unchecked("t"))),
                    Box::new(Expression::Literal(Literal::new_typed_literal("2022-06-01T08:46:53", xsd::DATE_TIME))),
                ),
                context: Context::new(),
            }],
        };

        let table = TimeSeriesTable {
            schema: Some("s3.otit-benchmark".into()),
            time_series_table: "timeseries_double".into(),
            value_column: "value".into(),
            timestamp_column: "timestamp".into(),
            identifier_column: "dir3".into(),
            value_datatype: NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#double"),
            year_column: Some("dir0".to_string()),
            month_column: Some("dir1".to_string()),
            day_column: Some("dir2".to_string()),
        };

        let sql_query = table.create_query(&tsq).unwrap();
        //println!("{}", sql_query)
        assert_eq!(&sql_query, r#"SELECT "dir3" AS "id", "timestamp" AS "t", "value" AS "v" FROM "s3.otit-benchmark"."timeseries_double" WHERE "dir3" IN ('A', 'B') AND (("dir0" < 2022) OR (("dir0" = 2022) AND ("dir1" < 6)) OR ("dir0" = 2022) AND ("dir1" = 6) AND ("dir2" < 1) OR ("timestamp" <= '2022-06-01 08:46:53'))"#);
    }
}
