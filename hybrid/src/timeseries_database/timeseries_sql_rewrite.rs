use crate::constants::DATETIME_AS_SECONDS;
use crate::timeseries_query::TimeSeriesQuery;
use log::debug;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use polars::export::chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone, Utc};
use sea_query::{Alias, BinOper, ColumnRef, Function, PostgresQueryBuilder, Query, SimpleExpr};
use sea_query::{Expr as SeaExpr, Iden, UnOper, Value};
use spargebra::algebra::{AggregateExpression, Expression};
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
            inner_query.and_where(self.sparql_expression_to_sql_expression(
                &c.expression,
                &variable_column_name_map,
                None,
            )?);
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
                    .map(|x| ColumnRef::TableColumn(
                            Rc::new(inner_query_name.clone()),
                            Rc::new(Name::Column(x.as_str().to_string())),
                        ))
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

    fn sparql_expression_to_sql_expression(
        &self,
        e: &Expression,
        variable_column_name_map: &HashMap<String, String>,
        table_name: Option<&Name>,
    ) -> Result<SimpleExpr, TimeSeriesQueryToSQLError> {
        Ok(match e {
            Expression::Or(left, right) => self
                .sparql_expression_to_sql_expression(left, variable_column_name_map, table_name)?
                .or(self.sparql_expression_to_sql_expression(
                    right,
                    variable_column_name_map,
                    table_name,
                )?),
            Expression::Literal(l) => {
                let v = l.value();
                let value = match l.datatype() {
                    xsd::DOUBLE => Value::Double(Some(v.parse().unwrap())),
                    xsd::FLOAT => Value::Float(Some(v.parse().unwrap())),
                    xsd::INTEGER => Value::BigInt(Some(v.parse().unwrap())),
                    xsd::LONG => Value::BigInt(Some(v.parse().unwrap())),
                    xsd::INT => Value::Int(Some(v.parse().unwrap())),
                    xsd::UNSIGNED_INT => Value::Unsigned(Some(v.parse().unwrap())),
                    xsd::UNSIGNED_LONG => Value::BigUnsigned(Some(v.parse().unwrap())),
                    xsd::STRING => Value::String(Some(Box::new(v.to_string()))),
                    xsd::DATE_TIME => {
                        if let Ok(dt) = v.parse::<NaiveDateTime>() {
                            let dt_with_tz_utc: DateTime<Utc> = Utc.from_utc_datetime(&dt);
                            Value::ChronoDateTimeUtc(Some(Box::new(dt_with_tz_utc)))
                        } else if let Ok(dt) = v.parse::<DateTime<Utc>>() {
                            Value::ChronoDateTimeUtc(Some(Box::new(dt)))
                        } else {
                            todo!("Could not parse {}", v);
                        }
                    }
                    _ => {
                        return Err(TimeSeriesQueryToSQLError::UnknownDatatype(
                            l.datatype().as_str().to_string(),
                        ));
                    }
                };
                SimpleExpr::Value(value)
            }
            Expression::Variable(v) => {
                if let Some(found_v) = variable_column_name_map.get(v.as_str()) {
                    if let Some(name) = table_name {
                        SimpleExpr::Column(ColumnRef::TableColumn(
                            Rc::new(name.clone()),
                            Rc::new(Name::Column(v.as_str().to_string())),
                        ))
                    } else {
                        SimpleExpr::Column(ColumnRef::Column(Rc::new(Name::Column(
                            found_v.to_string()
                        ))))
                    }
                } else {
                    return Err(TimeSeriesQueryToSQLError::UnknownVariable(
                        v.as_str().to_string(),
                    ));
                }
            }
            Expression::And(left, right) => self
                .sparql_expression_to_sql_expression(left, variable_column_name_map, table_name)?
                .and(self.sparql_expression_to_sql_expression(
                    right,
                    variable_column_name_map,
                    table_name,
                )?),
            Expression::Equal(left, right) => self
                .sparql_expression_to_sql_expression(left, variable_column_name_map, table_name)?
                .equals(self.sparql_expression_to_sql_expression(
                    right,
                    variable_column_name_map,
                    table_name,
                )?),
            Expression::Greater(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    variable_column_name_map,
                    table_name,
                )?),
                BinOper::GreaterThan,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    variable_column_name_map,
                    table_name,
                )?),
            ),
            Expression::GreaterOrEqual(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    variable_column_name_map,
                    table_name,
                )?),
                BinOper::GreaterThanOrEqual,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    variable_column_name_map,
                    table_name,
                )?),
            ),
            Expression::Less(left, right) => {
                SimpleExpr::Binary(
                    Box::new(self.sparql_expression_to_sql_expression(
                        left,
                        variable_column_name_map,
                        table_name,
                    )?),
                    BinOper::SmallerThan,
                    Box::new(self.sparql_expression_to_sql_expression(
                        right,
                        variable_column_name_map,
                        table_name,
                    )?),
                )
            }
            Expression::LessOrEqual(left, right) => {
                SimpleExpr::Binary(
                    Box::new(self.sparql_expression_to_sql_expression(
                        left,
                        variable_column_name_map,
                        table_name,
                    )?),
                    BinOper::SmallerThanOrEqual,
                    Box::new(self.sparql_expression_to_sql_expression(
                        right,
                        variable_column_name_map,
                        table_name,
                    )?),
                ) //Note flipped directions
            }
            Expression::In(left, right) => {
                let simple_right = right.iter().map(|x| {
                    self.sparql_expression_to_sql_expression(
                        x,
                        variable_column_name_map,
                        table_name,
                    )
                });
                let mut simple_right_values = vec![];
                for v in simple_right {
                    if let Ok(SimpleExpr::Value(v)) = v {
                        simple_right_values.push(v);
                    } else if let Err(e) = v {
                        return Err(e);
                    } else {
                        return Err(TimeSeriesQueryToSQLError::FoundNonValueInInExpression);
                    }
                }
                SeaExpr::expr(self.sparql_expression_to_sql_expression(
                    left,
                    variable_column_name_map,
                    table_name,
                )?)
                .is_in(simple_right_values)
            }
            Expression::Add(left, right) => self
                .sparql_expression_to_sql_expression(left, variable_column_name_map, table_name)?
                .add(self.sparql_expression_to_sql_expression(
                    right,
                    variable_column_name_map,
                    table_name,
                )?),
            Expression::Subtract(left, right) => self
                .sparql_expression_to_sql_expression(left, variable_column_name_map, table_name)?
                .sub(self.sparql_expression_to_sql_expression(
                    right,
                    variable_column_name_map,
                    table_name,
                )?),
            Expression::Multiply(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    variable_column_name_map,
                    table_name,
                )?),
                BinOper::Mul,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    variable_column_name_map,
                    table_name,
                )?),
            ),
            Expression::Divide(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    variable_column_name_map,
                    table_name,
                )?),
                BinOper::Div,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    variable_column_name_map,
                    table_name,
                )?),
            ),
            Expression::UnaryPlus(inner) => self.sparql_expression_to_sql_expression(
                inner,
                variable_column_name_map,
                table_name,
            )?,
            Expression::UnaryMinus(inner) => SimpleExpr::Value(Value::Double(Some(0.0))).sub(
                self.sparql_expression_to_sql_expression(
                    inner,
                    variable_column_name_map,
                    table_name,
                )?,
            ),
            Expression::Not(inner) => SimpleExpr::Unary(
                UnOper::Not,
                Box::new(self.sparql_expression_to_sql_expression(
                    inner,
                    variable_column_name_map,
                    table_name,
                )?),
            ),
            Expression::FunctionCall(f, expressions) => match f {
                spargebra::algebra::Function::Floor => {
                    let e = expressions.first().unwrap();
                    let mapped_e = self.sparql_expression_to_sql_expression(
                        e,
                        variable_column_name_map,
                        table_name,
                    )?;
                    SimpleExpr::FunctionCall(
                        Function::Custom(Rc::new(Name::Function("FLOOR".to_string()))),
                        vec![mapped_e],
                    )
                }
                spargebra::algebra::Function::Custom(c) => {
                    let e = expressions.first().unwrap();
                    let mapped_e = self.sparql_expression_to_sql_expression(
                        e,
                        variable_column_name_map,
                        table_name,
                    )?;
                    if c.as_str() == DATETIME_AS_SECONDS {
                        SimpleExpr::FunctionCall(
                            Function::Custom(Rc::new(Name::Function("UNIX_TIMESTAMP".to_string()))),
                            vec![mapped_e, SimpleExpr::Value(Value::String(Some(Box::new("YYYY-MM-DD HH:MI:SS.FFF".to_string()))))],
                        )
                    } else {
                        todo!("Fix custom {}", c)
                    }
                }
                _ => {
                    todo!("{}", f)
                }
            },
            _ => {
                unimplemented!("")
            }
        })
    }
    fn sparql_aggregate_expression_to_sql_expression(
        &self,
        agg: &AggregateExpression,
        variable_column_name_map: &HashMap<String, String>,
        table_name: Option<&Name>,
    ) -> Result<SimpleExpr, TimeSeriesQueryToSQLError> {
        Ok(match agg {
            AggregateExpression::Count { expr, distinct } => {
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
            AggregateExpression::Sum { expr, distinct } => SimpleExpr::FunctionCall(
                Function::Sum,
                vec![self.sparql_expression_to_sql_expression(
                    expr,
                    &variable_column_name_map,
                    table_name,
                )?],
            ),
            AggregateExpression::Avg { expr, distinct } => SimpleExpr::FunctionCall(
                Function::Avg,
                vec![self.sparql_expression_to_sql_expression(
                    expr,
                    &variable_column_name_map,
                    table_name,
                )?],
            ),
            AggregateExpression::Min { expr, distinct } => SimpleExpr::FunctionCall(
                Function::Min,
                vec![self.sparql_expression_to_sql_expression(
                    expr,
                    &variable_column_name_map,
                    table_name,
                )?],
            ),
            AggregateExpression::Max { expr, distinct } => SimpleExpr::FunctionCall(
                Function::Max,
                vec![self.sparql_expression_to_sql_expression(
                    expr,
                    &variable_column_name_map,
                    table_name,
                )?],
            ),
            AggregateExpression::GroupConcat {
                expr,
                distinct,
                separator,
            } => {
                todo!("")
            }
            AggregateExpression::Sample { expr, distinct } => {
                todo!("")
            }
            AggregateExpression::Custom {
                expr,
                distinct,
                name,
            } => {
                todo!("")
            }
        })
    }
}

#[derive(Clone)]
enum Name {
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
