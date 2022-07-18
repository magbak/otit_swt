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
use arrow_format::flight::data::{BasicAuth, FlightDescriptor, FlightInfo, HandshakeRequest};
use arrow_format::flight::service::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use flight_sql::CommandStatementQuery;

use polars::frame::DataFrame;
use polars_core::utils::accumulate_dataframes_vertical;
use prost::Message;

use crate::timeseries_database::timeseries_sql_rewrite::{
    TimeSeriesQueryToSQLError, TimeSeriesTable,
};
use std::error::Error;
use std::fmt::{Display, Formatter};
use thiserror::Error;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tonic::metadata::{ MetadataValue};
use crate::timeseries_database::arrow_flight_sql_database::flight_sql::CommandGetSqlInfo;

#[derive(Error, Debug)]
pub enum ArrowFlightSQLError {
    TonicStatus(#[from] Status),
    TransportError(#[from] tonic::transport::Error),
    TranslationError(#[from] TimeSeriesQueryToSQLError),
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
    conn: Channel,
    bearer_token: String,
    time_series_tables: Vec<TimeSeriesTable>,
}

impl ArrowFlightSQLDatabase {
    pub async fn new(
        endpoint: &str,
        time_series_tables: Vec<TimeSeriesTable>,
    ) -> Result<ArrowFlightSQLDatabase, ArrowFlightSQLError> {
        let conn = tonic::transport::Endpoint::new(endpoint.to_string())?.connect().await?;
        let mut client = FlightServiceClient::new(conn.clone());
        let basic_auth = BasicAuth{ username: "dremio".to_string(), password: "dremio123".to_string() };
        let mut authvec = vec![];
        basic_auth.encode(&mut authvec).unwrap();
        let handshake_request = HandshakeRequest{ protocol_version: 0, payload: authvec };
        let handshake_request_streaming = tokio_stream::iter(vec![handshake_request]);
        let hands = client.handshake(handshake_request_streaming).await.unwrap();
        let mut stream_hands = hands.into_inner();
        let mut token = None;
        for h in stream_hands.next().await {
            let h_ok = h.unwrap();
            token = Some(h_ok.payload.clone());
            println!("{:?}", h_ok.payload);
        }
        let token = token.take().unwrap();
        let str_token = std::str::from_utf8(token.as_slice()).unwrap();
        let bearer_token = format!("Bearer {}", str_token);
        println!("Strtoken: {str_token}");

        Ok(ArrowFlightSQLDatabase {
            conn,
            bearer_token,
            time_series_tables,
        })
    }

    pub async fn execute_sql_query(
        &mut self,
        query: String,
    ) -> Result<DataFrame, ArrowFlightSQLError> {
        let mut dfs = vec![];
        let query_cmd = CommandStatementQuery {
            query,
        };
        let query_encoding = query_cmd.encode_to_vec();
        println!("encoded: {:?}", query_encoding);
        let request = FlightDescriptor {
            r#type: 2, //CMD
            cmd: query_encoding,
            path: vec![], // Should be empty when CMD
        };

        let bearer_token = self.bearer_token.clone();
        let mut client= FlightServiceClient::with_interceptor(self.conn.clone(),|mut req: Request<()>| {
            let bearer_token: MetadataValue<_> = bearer_token.parse().unwrap();
            req.metadata_mut().insert("authorization", bearer_token);
            Ok(req)
        });

        let resp = client.get_flight_info(CommandGetSqlInfo{ info: vec![] }).await;

        let respose_result = client
            .get_flight_info(request)
            .await;
        let res = match respose_result {
            Ok(resp) => {resp}
            Err(err) => {
                println!("Err message: {}", err.message());
                panic!("bad! {:?}", err);
            }
        };
        let mut schema_opt = None;
        let mut ipc_schema_opt = None;
        for endpoint in res.into_inner().endpoint {
            if let Some(ticket) = &endpoint.ticket {
                let stream = client
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
        let mut query_string = None;
        if let Some(tsq_datatype) = &tsq.datatype {
            for table in &self.time_series_tables {
                if table.value_datatype.as_str() == tsq_datatype.as_str() {
                    query_string = Some(table.create_query(tsq)?);
                }
            }
            if query_string.is_none() {
                return Err(Box::new(ArrowFlightSQLError::DatatypeNotSupported(
                    tsq_datatype.as_str().to_string(),
                )));
            }
        } else {
            return Err(Box::new(
                ArrowFlightSQLError::MissingTimeseriesQueryDatatype,
            ));
        }

        Ok(self.execute_sql_query(query_string.unwrap()).await.unwrap())
    }
}
