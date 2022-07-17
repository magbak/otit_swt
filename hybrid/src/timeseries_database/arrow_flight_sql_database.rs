//Snippets from and inspired by: https://github.com/timvw/arrow-flight-sql-client/blob/main/src/client.rs

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod flight_sql {
    tonic::include_proto!("arrow.flight.protocol.sql");
}

use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::TimeSeriesQuery;
use arrow2::io::flight as flight2;
use arrow_format::flight::data::FlightDescriptor;
use arrow_format::flight::service::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use flight_sql::CommandStatementQuery;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use polars::export::chrono;
use polars::frame::DataFrame;
use polars_core::utils::accumulate_dataframes_vertical;
use prost::Message;
use sea_query::{Alias, BinOper, PostgresQueryBuilder, Query, SimpleExpr};
use sea_query::{Expr as SeaExpr, Iden, UnOper, Value};
use spargebra::algebra::Expression;
use std::error::Error;
use std::fmt::{Display, Formatter, Write};
use std::str::FromStr;
use thiserror::Error;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Status;

#[derive(Error, Debug)]
pub enum ArrowFlightSQLError {
    TonicStatus(#[from] Status),
    TransportError(#[from] tonic::transport::Error),
    TranslationError(String),
    DatatypeNotSupported(String),
    MissingTimeseriesQueryDatatype,
}

impl Display for ArrowFlightSQLError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowFlightSQLError::TonicStatus(status) => {
                write!(f, "Error with status: {}", status)
            }
            ArrowFlightSQLError::TransportError(err) => {
                write!(f, "Error during transport: {}", err)
            }
            ArrowFlightSQLError::TranslationError(s) => {
                write!(f, "Error during query translation: {}", s)
            }
            ArrowFlightSQLError::DatatypeNotSupported(dt) => {
                write!(f, "Datatype not supported: {}", dt)
            }
            ArrowFlightSQLError::MissingTimeseriesQueryDatatype => {
                write!(f, "Timeseries value datatype missing")
            }
        }
    }
}

pub struct ArrowFlightSQLDatabase {
    client: FlightServiceClient<Channel>,
    time_series_table: Name,
    value_columns: Vec<(Name, NamedNode)>, //Column name, Datatype
    timestamp_column: Name,
    identifier_column: Name,
}

impl ArrowFlightSQLDatabase {
    pub async fn new(
        endpoint: &str,
        time_series_table: &str,
        value_columns: Vec<(&str, &NamedNode)>,
        timestamp_column: &str,
        identifier_column: &str,
    ) -> Result<ArrowFlightSQLDatabase, ArrowFlightSQLError> {
        let client = FlightServiceClient::connect(endpoint.to_string())
            .await
            .map_err(ArrowFlightSQLError::from)?;
        Ok(ArrowFlightSQLDatabase {
            client,
            time_series_table: Name::Table(time_series_table.to_string()),
            value_columns: value_columns
                .iter()
                .map(|(c, dt)| (Name::Column(c.to_string()), (*dt).clone()))
                .collect(),
            timestamp_column: Name::Column(timestamp_column.to_string()),
            identifier_column: Name::Column(identifier_column.to_string()),
        })
    }

    pub fn create_query(&self, tsq: &TimeSeriesQuery) -> Result<String, ArrowFlightSQLError> {
        let mut query = Query::select();
        let mut use_value = None;
        if let Some(tsq_datatype) = &tsq.datatype {
            for (name, datatype) in &self.value_columns {
                if tsq_datatype.as_str() == datatype.as_str() {
                    query.expr_as(SeaExpr::col(name.clone()), Alias::new("value"));
                    use_value = Some(name.clone());
                }
            }
            if use_value.is_none() {
                return Err(ArrowFlightSQLError::DatatypeNotSupported(
                    tsq_datatype.as_str().to_string(),
                ));
            }
        } else {
            return Err(ArrowFlightSQLError::MissingTimeseriesQueryDatatype);
        }
        query
            .expr_as(
                SeaExpr::col(self.identifier_column.clone()),
                Alias::new("id"),
            )
            .expr_as(
                SeaExpr::col(self.timestamp_column.clone()),
                Alias::new("timestamp"),
            )
            .from(self.time_series_table.clone());

        if let Some(ids) = &tsq.ids {
            query.and_where(
                SeaExpr::col(self.identifier_column.clone()).is_in(
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
                &use_value.as_ref().unwrap(),
                timestamp_variable,
                value_variable,
            )?);
        }
        //TODO:Grouping/aggregation
        Ok(query.to_string(PostgresQueryBuilder))
    }

    fn sparql_expression_to_sql_expression(
        &self,
        e: &Expression,
        use_value: &Name,
        timestamp_variable: &Variable,
        value_variable: &Variable,
    ) -> Result<SimpleExpr, ArrowFlightSQLError> {
        Ok(match e {
            Expression::Or(left, right) => self
                .sparql_expression_to_sql_expression(
                    left,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?
                .or(self.sparql_expression_to_sql_expression(
                    right,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
            Expression::Literal(l) => {
                let v = l.value();
                match l.datatype() {
                    xsd::DOUBLE => SimpleExpr::Value(Value::Double(Some(v.parse().unwrap()))),
                    xsd::DATE_TIME => SimpleExpr::Value(Value::ChronoDateTimeWithTimeZone(Some(
                        Box::new(chrono::DateTime::from_str(v).unwrap()),
                    ))),
                    _ => {
                        return Err(ArrowFlightSQLError::TranslationError(format!(
                            "Unknown datatype: {}",
                            l.datatype()
                        )))
                    }
                }
            }
            Expression::Variable(v) => {
                if v.as_str() == &self.timestamp_column.to_string() {
                    SeaExpr::col(self.timestamp_column.clone()).into_simple_expr()
                } else if v.as_str() == use_value.to_string() {
                    SeaExpr::col(use_value.clone()).into_simple_expr()
                } else {
                    return Err(ArrowFlightSQLError::TranslationError(format!(
                        "Unknown variable: {}",
                        v
                    )));
                }
            }
            Expression::And(left, right) => self
                .sparql_expression_to_sql_expression(
                    left,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?
                .and(self.sparql_expression_to_sql_expression(
                    right,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
            Expression::Equal(left, right) => self
                .sparql_expression_to_sql_expression(
                    left,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?
                .equals(self.sparql_expression_to_sql_expression(
                    right,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
            Expression::Greater(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
                BinOper::GreaterThan,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
            ),
            Expression::GreaterOrEqual(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
                BinOper::GreaterThanOrEqual,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
            ),
            Expression::Less(left, right) => {
                SimpleExpr::Binary(
                    Box::new(self.sparql_expression_to_sql_expression(
                        right,
                        use_value,
                        timestamp_variable,
                        value_variable,
                    )?),
                    BinOper::GreaterThan,
                    Box::new(self.sparql_expression_to_sql_expression(
                        left,
                        use_value,
                        timestamp_variable,
                        value_variable,
                    )?),
                ) //Note flipped directions
            }
            Expression::LessOrEqual(left, right) => {
                SimpleExpr::Binary(
                    Box::new(self.sparql_expression_to_sql_expression(
                        right,
                        use_value,
                        timestamp_variable,
                        value_variable,
                    )?),
                    BinOper::GreaterThanOrEqual,
                    Box::new(self.sparql_expression_to_sql_expression(
                        left,
                        use_value,
                        timestamp_variable,
                        value_variable,
                    )?),
                ) //Note flipped directions
            }
            Expression::In(left, right) => {
                let simple_right = right.iter().map(|x| {
                    self.sparql_expression_to_sql_expression(
                        x,
                        use_value,
                        timestamp_variable,
                        value_variable,
                    )
                });
                let mut simple_right_values = vec![];
                for v in simple_right {
                    if let Ok(SimpleExpr::Value(v)) = v {
                        simple_right_values.push(v);
                    } else if let Err(e) = v {
                        return Err(e);
                    } else {
                        return Err(ArrowFlightSQLError::TranslationError(
                            "Expected value in IN-expression".to_string(),
                        ));
                    }
                }
                SeaExpr::expr(self.sparql_expression_to_sql_expression(
                    left,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?)
                .is_in(simple_right_values)
            }
            Expression::Add(left, right) => self
                .sparql_expression_to_sql_expression(
                    left,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?
                .add(self.sparql_expression_to_sql_expression(
                    right,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
            Expression::Subtract(left, right) => self
                .sparql_expression_to_sql_expression(
                    left,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?
                .sub(self.sparql_expression_to_sql_expression(
                    right,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
            Expression::Multiply(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
                BinOper::Mul,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
            ),
            Expression::Divide(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(
                    left,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
                BinOper::Div,
                Box::new(self.sparql_expression_to_sql_expression(
                    right,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?),
            ),
            Expression::UnaryPlus(inner) => self.sparql_expression_to_sql_expression(
                inner,
                use_value,
                timestamp_variable,
                value_variable,
            )?,
            Expression::UnaryMinus(inner) => SimpleExpr::Value(Value::Double(Some(0.0))).sub(
                self.sparql_expression_to_sql_expression(
                    inner,
                    use_value,
                    timestamp_variable,
                    value_variable,
                )?,
            ),
            Expression::Not(inner) => SimpleExpr::Unary(
                UnOper::Not,
                Box::new(self.sparql_expression_to_sql_expression(
                    inner,
                    use_value,
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

    pub async fn execute_sql_query(
        &mut self,
        query: String,
    ) -> Result<DataFrame, ArrowFlightSQLError> {
        let mut dfs = vec![];
        let mut query_encoding = vec![];
        let query_cmd = CommandStatementQuery {
            query: query.to_string(),
        };
        query_encoding.reserve(query_cmd.encoded_len());
        query_cmd.encode(&mut query_encoding).unwrap();

        let request = FlightDescriptor {
            r#type: 2, //CMD
            cmd: query_encoding,
            path: vec![], // Should be empty when CMD
        };
        let res = self
            .client
            .get_flight_info(request)
            .await
            .map_err(ArrowFlightSQLError::from)?;
        let mut schema_opt = None;
        let mut ipc_schema_opt = None;
        for endpoint in res.into_inner().endpoint {
            if let Some(ticket) = &endpoint.ticket {
                let stream = self
                    .client
                    .do_get(ticket.clone())
                    .await
                    .map_err(ArrowFlightSQLError::from)?;
                let mut streaming_flight_data = stream.into_inner();
                while let Some(flight_data_result) = streaming_flight_data.next().await {
                    if let Ok(flight_data) = flight_data_result {
                        if schema_opt.is_none() || ipc_schema_opt.is_none() {
                            let schemas_result =
                                flight2::deserialize_schemas(&flight_data.data_header);
                            match schemas_result {
                                Ok((schema, ipc_schema)) => {
                                    schema_opt = Some(schema);
                                    ipc_schema_opt = Some(ipc_schema);
                                }
                                Err(err) => {
                                    panic!("Fiks dette")
                                }
                            }
                        }
                        let chunk_result = flight2::deserialize_batch(
                            &flight_data,
                            schema_opt.as_ref().unwrap().fields.as_slice(),
                            &ipc_schema_opt.as_ref().unwrap(),
                            &Default::default(),
                        );
                        match chunk_result {
                            Ok(ch) => {
                                let df = DataFrame::try_from((
                                    ch,
                                    schema_opt.as_ref().unwrap().fields.as_slice(),
                                ))
                                .unwrap(); //TODO:handle
                                dfs.push(df)
                            }
                            Err(err) => {
                                panic!("Fixit")
                            }
                        }
                    }
                }
            }
        }
        Ok(accumulate_dataframes_vertical(dfs).unwrap()) //TODO: handle
    }
}

#[async_trait]
impl TimeSeriesQueryable for ArrowFlightSQLDatabase {
    async fn execute(&mut self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        let query_string = self.create_query(tsq)?;
        Ok(self.execute_sql_query(query_string).await.unwrap())
    }
}

#[derive(Clone)]
enum Name {
    Table(String),
    Column(String),
}

impl Iden for Name {
    fn unquoted(&self, s: &mut dyn Write) {
        write!(
            s,
            "{}",
            match self {
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
