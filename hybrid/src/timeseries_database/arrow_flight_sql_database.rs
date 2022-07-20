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
use arrow_format::flight::data::{FlightDescriptor, HandshakeRequest};
use async_trait::async_trait;

use polars::frame::DataFrame;
use polars_core::utils::accumulate_dataframes_vertical;

use crate::timeseries_database::timeseries_sql_rewrite::{
    TimeSeriesQueryToSQLError, TimeSeriesTable,
};
use std::error::Error;
use std::fmt::{Display, Formatter};
use arrow_format::flight::service::flight_service_client::FlightServiceClient;
use arrow_format::ipc::planus::ReadAsRoot;
use arrow_format::ipc::{MessageHeaderRef};
use thiserror::Error;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{IntoRequest, Request, Status};
use tonic::metadata::{ MetadataValue};

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
    client: FlightServiceClient<Channel>,
    token: String,
    time_series_tables: Vec<TimeSeriesTable>,
}

impl ArrowFlightSQLDatabase {
    pub async fn new(
        endpoint: &str,
        time_series_tables: Vec<TimeSeriesTable>,
    ) -> Result<ArrowFlightSQLDatabase, ArrowFlightSQLError> {
        let conn = tonic::transport::Endpoint::new(endpoint.to_string())?.connect().await?;
        let token = authenticate(conn.clone(), "dremio", "dremio123").await?;
        let client = FlightServiceClient::new(conn);

        Ok(ArrowFlightSQLDatabase {
            client,
            token,
            time_series_tables,
        })
    }

    pub async fn execute_sql_query(
        &mut self,
        query: String,
    ) -> Result<DataFrame, ArrowFlightSQLError> {
        let mut dfs = vec![];
        let mut request = FlightDescriptor {
            r#type: 2, //CMD
            cmd: query.into_bytes(),
            //TODO: For some reason, encoding the CommandStatementQuery-struct
            // gives me a parsing error with an extra character at the start of the decoded query.
            path: vec![], // Should be empty when CMD
        }.into_request();
        add_auth_header(&mut request, &self.token);

        let respose_result = self.client
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
            if let Some(ticket) = endpoint.ticket.clone() {
                let mut ticket = ticket.into_request();
                add_auth_header(&mut ticket, &self.token);
                let stream = self.client
                    .do_get(ticket)
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
                        let message = arrow_format::ipc::MessageRef::read_as_root(&flight_data.data_header).unwrap();
                        let header = message.header().unwrap().unwrap();
                        match header {
                            MessageHeaderRef::Schema(s) => {
                                println!("Received schema, should be handled already: {:?}", s);
                                //TODO: Move schema code block here..
                            }
                            MessageHeaderRef::DictionaryBatch(db) => {
                                todo!("Handle dictionary {:?}", db);
                            }
                            MessageHeaderRef::RecordBatch(_) => {
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
                                        panic!("Fixit {}", err);
                                    }
                                }
                            }
                            MessageHeaderRef::Tensor(_) => {},//TODO handle?
                            MessageHeaderRef::SparseTensor(_) => {}//Todo handle?
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

//Adapted from: https://github.com/apache/arrow-rs/blob/master/integration-testing/src/flight_client_scenarios/auth_basic_proto.rs
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

async fn authenticate(
    conn: Channel,
    username: &str,
    password: &str,
) -> Result<String, ArrowFlightSQLError> {
    let handshake_request = HandshakeRequest {
        protocol_version: 2,
        payload:vec![],
        };
    let user_pass_string = format!("{}:{}", username, password);
    let user_pass_bytes = user_pass_string.as_bytes();
    let base64_bytes = base64::encode(user_pass_bytes);
    let basic_auth = format!("Basic {}", base64_bytes);
    let mut client = FlightServiceClient::with_interceptor(conn, |mut req: Request<()>| {
        req.metadata_mut().insert("authorization", basic_auth.parse().unwrap());
        Ok(req)
    });

    let handshake_request_streaming = tokio_stream::iter(vec![handshake_request]);

    let rx = client.handshake(handshake_request_streaming).await?;
    let bearer_token = rx.metadata().get("authorization").unwrap().to_str().unwrap().to_string();
    Ok(bearer_token)
}

fn add_auth_header<T>(request:&mut Request<T>, bearer_token: &str) {
    let token_value: MetadataValue<_> = bearer_token.parse().unwrap();
    request.metadata_mut().insert("authorization", token_value);
}