use oxrdf::vocab::xsd;
use polars::export::chrono::{DateTime, NaiveDateTime, Utc};
use sea_query::Expr as SeaExpr;
use sea_query::{BinOper, ColumnRef, Function, SimpleExpr, UnOper, Value};
use spargebra::algebra::Expression;
use std::collections::HashMap;
use std::rc::Rc;

use super::TimeSeriesTable;
use crate::constants::DATETIME_AS_SECONDS;
use crate::timeseries_database::timeseries_sql_rewrite::{Name, TimeSeriesQueryToSQLError};

impl TimeSeriesTable {
    pub(crate) fn sparql_expression_to_sql_expression(
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
                    xsd::BOOLEAN => Value::Bool(Some(v.parse().unwrap())),
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
                            Value::ChronoDateTime(Some(Box::new(dt)))
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
                            found_v.to_string(),
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
            Expression::Less(left, right) => SimpleExpr::Binary(
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
            ),
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
                            vec![
                                mapped_e,
                                SimpleExpr::Value(Value::String(Some(Box::new(
                                    "YYYY-MM-DD HH:MI:SS.FFF".to_string(),
                                )))),
                            ],
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
}
