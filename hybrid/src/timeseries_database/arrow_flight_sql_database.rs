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

use arrow_format::flight::service::flight_service_client::FlightServiceClient;
use arrow_format::flight::data::FlightDescriptor;
use flight_sql::CommandStatementQuery;
use prost::{bytes, Message};
use std::fmt::{Display, Formatter};
use polars::frame::{DataFrame};
use thiserror::Error;
use tonic::codegen::{Body, StdError};
use tonic::{ Status};
use tonic::transport::Channel;
use tokio_stream::StreamExt;
use arrow2::io::flight as flight2;
use polars_core::utils::accumulate_dataframes_vertical;

#[derive(Error, Debug)]
pub enum ArrowFlightSQLError {
    TonicStatus(#[from] Status),
    TransportError(#[from] tonic::transport::Error)
}

impl Display for ArrowFlightSQLError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowFlightSQLError::TonicStatus(status) => {
                write!(f, "Error with status: {}", status)
            },
            ArrowFlightSQLError::TransportError(err) => {
                write!(f, "Error during transport: {}", err)
            }
        }
    }
}

pub struct ArrowFlightSQLDatabase<T> {
    client: FlightServiceClient<T>,
}

impl<T> ArrowFlightSQLDatabase<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Default + Body<Data = bytes::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    pub async fn new(&mut self, endpoint:&str) -> Result<ArrowFlightSQLDatabase<Channel>, ArrowFlightSQLError> {
        let client = FlightServiceClient::connect(endpoint.to_string()).await.map_err(ArrowFlightSQLError::from)?;
        Ok(ArrowFlightSQLDatabase {client})
    }

    pub async fn execute_query(&mut self, query: &str) -> Result<DataFrame, ArrowFlightSQLError> {
        let mut dfs = vec![];
        let mut query_encoding = vec![];
        let query_cmd = CommandStatementQuery {
            query: "".to_string(),
        };
        query_encoding.reserve(query_cmd.encoded_len());
        query_cmd.encode(&mut query_encoding);

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
                let stream = self.client.do_get(ticket.clone()).await.map_err(ArrowFlightSQLError::from)?;
                let mut streaming_flight_data = stream.into_inner();
                while let Some(flight_data_result) = streaming_flight_data.next().await {
                    if let Ok(flight_data) = flight_data_result {
                        if schema_opt.is_none() || ipc_schema_opt.is_none() {
                            let schemas_result = flight2::deserialize_schemas(&flight_data.data_header);
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
                        let chunk_result = flight2::deserialize_batch(&flight_data, schema_opt.as_ref().unwrap().fields.as_slice(), &ipc_schema_opt.as_ref().unwrap(), &Default::default());
                        match chunk_result {
                            Ok(ch) => {
                                let df = DataFrame::try_from((ch, schema_opt.as_ref().unwrap().fields.as_slice())).unwrap(); //TODO:handle
                                dfs.push(df)
                            }
                            Err(err) => {panic!("Fixit")}
                        }
                    }
                }
            }
        }
        Ok(accumulate_dataframes_vertical(dfs).unwrap())//TODO: handle
    }
}
