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

use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::TimeSeriesQuery;
use arrow2::io::flight as flight2;
use arrow_format::flight::data::{FlightDescriptor, FlightInfo, HandshakeRequest};
use async_trait::async_trait;

use polars::frame::DataFrame;
use polars_core::utils::accumulate_dataframes_vertical;

use crate::timeseries_database::timeseries_sql_rewrite::{
    TimeSeriesQueryToSQLError, TimeSeriesTable,
};
use arrow_format::flight::service::flight_service_client::FlightServiceClient;
use arrow_format::ipc::planus::ReadAsRoot;
use arrow_format::ipc::MessageHeaderRef;
use log::{debug, warn};
use polars_core::error::ArrowError;
use polars_core::prelude::PolarsError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use thiserror::Error;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{IntoRequest, Request, Response, Status};

#[derive(Error, Debug)]
pub enum ArrowFlightSQLError {
    TonicStatus(#[from] Status),
    TransportError(#[from] tonic::transport::Error),
    TranslationError(#[from] TimeSeriesQueryToSQLError),
    DatatypeNotSupported(String),
    MissingTimeseriesQueryDatatype,
    ArrowError(#[from] ArrowError),
    PolarsError(#[from] PolarsError),
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
            ArrowFlightSQLError::ArrowError(err) => {
                write!(f, "Problem deserializing arrow: {}", err)
            }
            ArrowFlightSQLError::PolarsError(err) => {
                write!(f, "Problem creating dataframe from arrow: {:?}", err)
            }
        }
    }
}

pub struct ArrowFlightSQLDatabase {
    endpoint: String,
    username: String,
    password: String,
    conn: Option<Channel>,
    token: Option<String>,
    cookies: Option<Vec<String>>,
    time_series_tables: Vec<TimeSeriesTable>,
}

impl ArrowFlightSQLDatabase {
    pub async fn new(
        endpoint: &str,
        username: &str,
        password: &str,
        time_series_tables: Vec<TimeSeriesTable>,
    ) -> Result<ArrowFlightSQLDatabase, ArrowFlightSQLError> {
        let mut db = ArrowFlightSQLDatabase {
            endpoint: endpoint.into(),
            username: username.into(),
            password: password.into(),
            conn: None,
            token: None,
            cookies: None,
            time_series_tables,
        };
        db.init().await?;
        Ok(db)
    }

    async fn init(&mut self) -> Result<(),ArrowFlightSQLError> {
        let (token, conn) =
            authenticated_connection(&self.username, &self.password, &self.endpoint).await?;
        self.token = Some(token);
        self.conn = Some(conn);
        Ok(())
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
        }
        .into_request();
        add_auth_header(&mut request, self.token.as_ref().unwrap());

        let mut client = FlightServiceClient::new(self.conn.as_ref().unwrap().clone());
        let response = client.get_flight_info(request).await?;
        if self.cookies.is_none() {
            self.find_set_cookies(&response);
        }
        debug!("Got flight info response");
        let mut schema_opt = None;
        let mut ipc_schema_opt = None;
        for endpoint in response.into_inner().endpoint {
            if let Some(ticket) = endpoint.ticket.clone() {
                let mut ticket = ticket.into_request();
                add_auth_header(&mut ticket, self.token.as_ref().unwrap());
                add_cookies(&mut ticket, self.cookies.as_ref().unwrap());
                let stream = client
                    .do_get(ticket)
                    .await
                    .map_err(ArrowFlightSQLError::from)?;
                let mut streaming_flight_data = stream.into_inner();
                while let Some(flight_data_result) = streaming_flight_data.next().await {
                    if let Ok(flight_data) = flight_data_result {
                        let message =
                            arrow_format::ipc::MessageRef::read_as_root(&flight_data.data_header)
                                .unwrap();
                        let header = message.header().unwrap().unwrap();
                        match header {
                            MessageHeaderRef::Schema(_) => {
                                if schema_opt.is_some() || ipc_schema_opt.is_some() {
                                    warn!("Received multiple schema messages, keeping last");
                                }
                                let (schema, ipc_schema) =
                                    flight2::deserialize_schemas(&flight_data.data_header)
                                        .expect("Schema deserialization problem");
                                schema_opt = Some(schema);
                                ipc_schema_opt = Some(ipc_schema);
                            }
                            MessageHeaderRef::DictionaryBatch(_) => {
                                unimplemented!("Dictionary batch not implemented")
                            }
                            MessageHeaderRef::RecordBatch(_) => {
                                let chunk = flight2::deserialize_batch(
                                    &flight_data,
                                    schema_opt.as_ref().unwrap().fields.as_slice(),
                                    &ipc_schema_opt.as_ref().unwrap(),
                                    &Default::default(),
                                )
                                .map_err(ArrowFlightSQLError::from)?;

                                let df = DataFrame::try_from((
                                    chunk,
                                    schema_opt.as_ref().unwrap().fields.as_slice(),
                                ))
                                .map_err(ArrowFlightSQLError::from)?;
                                dfs.push(df);
                            }
                            MessageHeaderRef::Tensor(_) => {
                                unimplemented!("Tensor message not implemented");
                            }
                            MessageHeaderRef::SparseTensor(_) => {
                                unimplemented!("Sparse tensor message not implemented");
                            }
                        }
                    }
                }
            }
        }
        Ok(accumulate_dataframes_vertical(dfs).expect("Problem stacking dataframes"))
    }
    fn find_set_cookies(&mut self, response: &Response<FlightInfo>) {
        let mut cookies:Vec<String> = response
        .metadata()
        .get_all("set-cookie").iter().map(|x|x.to_str().unwrap().to_string()).collect();

        cookies = cookies.into_iter().map(|x|x.split(";").next().unwrap().to_string()).collect();
        self.cookies = Some(cookies);
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

        Ok(self.execute_sql_query(query_string.unwrap()).await?)
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

async fn authenticated_connection(
    username: &str,
    password: &str,
    endpoint: &str,
) -> Result<(String, Channel), ArrowFlightSQLError> {
    let conn = tonic::transport::Endpoint::new(endpoint.to_string())?
        .connect()
        .await?;
    let token = authenticate(conn.clone(), username, password).await?;
    Ok((token, conn))
}

async fn authenticate(
    conn: Channel,
    username: &str,
    password: &str,
) -> Result<String, ArrowFlightSQLError> {
    let handshake_request = HandshakeRequest {
        protocol_version: 2,
        payload: vec![],
    };
    let user_pass_string = format!("{}:{}", username, password);
    let user_pass_bytes = user_pass_string.as_bytes();
    let base64_bytes = base64::encode(user_pass_bytes);
    let basic_auth = format!("Basic {}", base64_bytes);
    let mut client = FlightServiceClient::with_interceptor(conn, |mut req: Request<()>| {
        req.metadata_mut()
            .insert("authorization", basic_auth.parse().unwrap());
        Ok(req)
    });

    let handshake_request_streaming = tokio_stream::iter(vec![handshake_request]);

    let rx = client.handshake(handshake_request_streaming).await?;
    let bearer_token = rx
        .metadata()
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    Ok(bearer_token)
}

fn add_auth_header<T>(request: &mut Request<T>, bearer_token: &str) {
    let token_value: MetadataValue<_> = bearer_token.parse().unwrap();
    request.metadata_mut().insert("authorization", token_value);
}

fn add_cookies<T>(request: &mut Request<T>, cookies: &Vec<String>) {
    let cookies_string = cookies.join("; ");
    let cookie_value: MetadataValue<_> = cookies_string.parse().unwrap();
    request.metadata_mut().append("cookie", cookie_value);
}

