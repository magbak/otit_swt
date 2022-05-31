use reqwest::header::CONTENT_TYPE;
use reqwest::{Error, StatusCode};
use sparesults::{
    ParseError, QueryResultsFormat, QueryResultsParser, QueryResultsReader, QuerySolution,
};
use spargebra::Query;

#[derive(Debug)]
pub struct QueryExecutionError {
    kind: QueryExecutionErrorKind,
}

#[derive(Debug)]
pub enum QueryExecutionErrorKind {
    RequestError(Error),
    BadStatusCode(StatusCode),
    ResultsParseError(ParseError),
    SolutionParseError(ParseError),
    WrongResultType,
}

pub async fn execute_sparql_query(
    endpoint: &str,
    query: Query,
) -> Result<Vec<QuerySolution>, QueryExecutionError> {
    let client = reqwest::Client::new();
    let response = client
        .post(endpoint)
        .header(CONTENT_TYPE, "application/sparql-query")
        .body(query.to_string())
        .send()
        .await;
    match response {
        Ok(proper_response) => {
            if proper_response.status().as_u16() != 200 {
                Err(QueryExecutionError {
                    kind: QueryExecutionErrorKind::BadStatusCode(proper_response.status()),
                })
            } else {
                let text = proper_response.text().await.expect("Read text error");
                let json_parser = QueryResultsParser::from_format(QueryResultsFormat::Json);
                let parsed_results = json_parser.read_results(text.as_bytes());
                match parsed_results {
                    Ok(reader) => {
                        let mut solns = vec![];
                        if let QueryResultsReader::Solutions(solutions) = reader {
                            for s in solutions {
                                match s {
                                    Ok(query_solution) => solns.push(query_solution),
                                    Err(parse_error) => {
                                        return Err(QueryExecutionError {
                                            kind: QueryExecutionErrorKind::SolutionParseError(
                                                parse_error,
                                            ),
                                        })
                                    }
                                }
                            }
                            Ok(solns)
                        } else {
                            Err(QueryExecutionError {
                                kind: QueryExecutionErrorKind::WrongResultType,
                            })
                        }
                    }
                    Err(parse_error) => Err(QueryExecutionError {
                        kind: QueryExecutionErrorKind::ResultsParseError(parse_error),
                    }),
                }
            }
        }
        Err(error) => Err(QueryExecutionError {
            kind: QueryExecutionErrorKind::RequestError(error),
        }),
    }
}
