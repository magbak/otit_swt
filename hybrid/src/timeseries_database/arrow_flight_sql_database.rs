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

pub mod flight {
    tonic::include_proto!("arrow.flight.protocol");
}
pub mod flight_sql {
    tonic::include_proto!("arrow.flight.protocol.sql");
}

use flight::flight_service_client::FlightServiceClient;
use flight::FlightDescriptor;
use flight_sql::CommandStatementQuery;
use prost::{bytes, Message};
use std::fmt::{Display, Formatter};
use thiserror::Error;
use tonic::codegen::{Body, StdError};
use tonic::{Response, Status};

#[derive(Error, Debug)]
pub enum ArrowFlightSQLError {
    TonicStatus(#[from] Status),
}

impl Display for ArrowFlightSQLError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowFlightSQLError::TonicStatus(status) => {
                write!(f, "TonicStatus:{}", status)
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
    pub async fn execute(&mut self, query: &str) -> Result<(), ArrowFlightSQLError> {
        let mut query_encoding = vec![];
        let query_cmd = CommandStatementQuery {
            query: "".to_string(),
        };
        query_encoding.reserve(query_cmd.encoded_len());
        query_cmd.encode(&mut query_encoding);

        let request = FlightDescriptor {
            r#type: 2, //CMD
            cmd: query_encoding,
            path: vec![], // Should be empty
        };
        let res = self
            .client
            .get_flight_info(request)
            .await
            .map_err(ArrowFlightSQLError::from)?;
        Ok(())
    }
}
