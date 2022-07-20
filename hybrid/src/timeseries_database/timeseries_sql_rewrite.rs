use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use sea_query::{Alias, BinOper, PostgresQueryBuilder, Query, SimpleExpr};
use sea_query::{Expr as SeaExpr, Iden, UnOper, Value};
use spargebra::algebra::Expression;
use std::error::Error;
use std::fmt::{Display, Formatter, Write};
use polars::export::chrono::NaiveDateTime;

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
        let mut query = Query::select();
        query
            .expr_as(
                SeaExpr::col(Name::Column(self.identifier_column.clone())),
                Alias::new(tsq.identifier_variable.as_ref().unwrap().as_str()),
            )
            .expr_as(
                SeaExpr::col(Name::Column(self.value_column.clone())),
                Alias::new(tsq.value_variable.as_ref().unwrap().variable.as_str()),
            )
            .expr_as(
                SeaExpr::col(Name::Column(self.timestamp_column.clone())),
                Alias::new(tsq.timestamp_variable.as_ref().unwrap().variable.as_str()),
            );
        if let Some(schema) = &self.schema {
            query.from((Name::Schema(schema.clone()), Name::Table(self.time_series_table.clone())));
        } else {
            query.from(Name::Table(self.time_series_table.clone()));
        }

        if let Some(ids) = &tsq.ids {
            query.and_where(
                SeaExpr::col(Name::Column(self.identifier_column.clone())).is_in(
                    ids.iter()
                        .map(|x| Value::String(Some(Box::new(x.to_string())))),
                ),
            );
        }
        let timestamp_variable = &tsq.timestamp_variable.as_ref().unwrap().variable;
        let value_variable = &tsq.value_variable.as_ref().unwrap().variable;

        for c in &tsq.conditions {
            query.and_where(self.sparql_expression_to_sql_expression(
                &c.expression,
                timestamp_variable,
                value_variable,
            )?);
        }
        //TODO:Grouping/aggregation
        let query_string = query.to_string(PostgresQueryBuilder);
        Ok(query_string)
    }

    fn sparql_expression_to_sql_expression(
        &self,
        e: &Expression,
        timestamp_variable: &Variable,
        value_variable: &Variable,
    ) -> Result<SimpleExpr, TimeSeriesQueryToSQLError> {
        Ok(match e {
            Expression::Or(left, right) => self
                .sparql_expression_to_sql_expression(left, timestamp_variable, value_variable)?
                .or(self.sparql_expression_to_sql_expression(
                    right,
                    timestamp_variable,
                    value_variable,
                )?),
            Expression::Literal(l) => {
                let v = l.value();
                match l.datatype() {
                    xsd::DOUBLE => SimpleExpr::Value(Value::Double(Some(v.parse().unwrap()))),
                    xsd::INTEGER => SimpleExpr::Value(Value::BigInt(Some(v.parse().unwrap()))),
                    xsd::DATE_TIME => {
                        let dt = v
                            .parse::<NaiveDateTime>()
                            .expect("Datetime parsing error");
                        SimpleExpr::Value(Value::ChronoDateTime(Some(
                            Box::new(dt))))
                    }
                    _ => {
                        return Err(TimeSeriesQueryToSQLError::UnknownDatatype(
                            l.datatype().as_str().to_string(),
                        ));
                    }
                }
            }
            Expression::Variable(v) => {
                if v.as_str() == timestamp_variable.as_str() {
                    SeaExpr::col(Name::Column(self.timestamp_column.clone())).into_simple_expr()
                } else if v.as_str() == value_variable.as_str() {
                    SeaExpr::col(Name::Column(self.value_column.clone())).into_simple_expr()
                } else {
                    return Err(TimeSeriesQueryToSQLError::UnknownVariable(
                        v.as_str().to_string(),
                    ));
                }
            }
            Expression::And(left, right) => self
                .sparql_expression_to_sql_expression(left, timestamp_variable, value_variable)?
                .and(self.sparql_expression_to_sql_expression(
                    right,
                    timestamp_variable,
                    value_variable,
                )?),
            Expression::Equal(left, right) => self
                .sparql_expression_to_sql_expression(left, timestamp_variable, value_variable)?
                .equals(self.sparql_expression_to_sql_expression(
                    right,
                    timestamp_variable,
                    value_variable,
                )?),
            Expression::Greater(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    timestamp_variable,
                    value_variable,
                )?),
                BinOper::GreaterThan,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    timestamp_variable,
                    value_variable,
                )?),
            ),
            Expression::GreaterOrEqual(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    timestamp_variable,
                    value_variable,
                )?),
                BinOper::GreaterThanOrEqual,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    timestamp_variable,
                    value_variable,
                )?),
            ),
            Expression::Less(left, right) => {
                SimpleExpr::Binary(
                    Box::new(self.sparql_expression_to_sql_expression(
                        right,
                        timestamp_variable,
                        value_variable,
                    )?),
                    BinOper::GreaterThan,
                    Box::new(self.sparql_expression_to_sql_expression(
                        left,
                        timestamp_variable,
                        value_variable,
                    )?),
                ) //Note flipped directions
            }
            Expression::LessOrEqual(left, right) => {
                SimpleExpr::Binary(
                    Box::new(self.sparql_expression_to_sql_expression(
                        right,
                        timestamp_variable,
                        value_variable,
                    )?),
                    BinOper::GreaterThanOrEqual,
                    Box::new(self.sparql_expression_to_sql_expression(
                        left,
                        timestamp_variable,
                        value_variable,
                    )?),
                ) //Note flipped directions
            }
            Expression::In(left, right) => {
                let simple_right = right.iter().map(|x| {
                    self.sparql_expression_to_sql_expression(x, timestamp_variable, value_variable)
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
                    timestamp_variable,
                    value_variable,
                )?)
                .is_in(simple_right_values)
            }
            Expression::Add(left, right) => self
                .sparql_expression_to_sql_expression(left, timestamp_variable, value_variable)?
                .add(self.sparql_expression_to_sql_expression(
                    right,
                    timestamp_variable,
                    value_variable,
                )?),
            Expression::Subtract(left, right) => self
                .sparql_expression_to_sql_expression(left, timestamp_variable, value_variable)?
                .sub(self.sparql_expression_to_sql_expression(
                    right,
                    timestamp_variable,
                    value_variable,
                )?),
            Expression::Multiply(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    timestamp_variable,
                    value_variable,
                )?),
                BinOper::Mul,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    timestamp_variable,
                    value_variable,
                )?),
            ),
            Expression::Divide(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    timestamp_variable,
                    value_variable,
                )?),
                BinOper::Div,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    timestamp_variable,
                    value_variable,
                )?),
            ),
            Expression::UnaryPlus(inner) => {
                self.sparql_expression_to_sql_expression(inner, timestamp_variable, value_variable)?
            }
            Expression::UnaryMinus(inner) => SimpleExpr::Value(Value::Double(Some(0.0))).sub(
                self.sparql_expression_to_sql_expression(
                    inner,
                    timestamp_variable,
                    value_variable,
                )?,
            ),
            Expression::Not(inner) => SimpleExpr::Unary(
                UnOper::Not,
                Box::new(self.sparql_expression_to_sql_expression(
                    inner,
                    timestamp_variable,
                    value_variable,
                )?),
            ),
            Expression::FunctionCall(_, _) => {
                todo!("")
            }
            _ => {
                unimplemented!("")
            }
        })
    }
}

#[derive(Clone)]
enum Name {
    Schema(String),
    Table(String),
    Column(String),
}

impl Iden for Name {
    fn unquoted(&self, s: &mut dyn Write) {
        write!(
            s,
            "{}",
            match self {
                Name::Schema(s) => {s}
                Name::Table(s) => {
                    s
                }
                Name::Column(s) => {
                    s
                }
            }
        )
        .unwrap();
    }
}
