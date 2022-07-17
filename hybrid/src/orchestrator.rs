use crate::combiner::Combiner;
use crate::groupby_pushdown::find_all_groupby_pushdowns;
use crate::preprocessing::Preprocessor;
use crate::rewriting::StaticQueryRewriter;
use crate::sparql_result_to_polars::create_static_query_result_df;
use crate::splitter::parse_sparql_select_query;
use crate::static_sparql::execute_sparql_query;
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::TimeSeriesQuery;
use log::debug;
use oxrdf::vocab::xsd;
use oxrdf::Term;
use polars::frame::DataFrame;
use sparesults::QuerySolution;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum OrchestrationError {
    InconsistentDatatype(String, String, String),
}

impl Display for OrchestrationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OrchestrationError::InconsistentDatatype(s1, s2, s3) => {
                write!(
                    f,
                    "Inconsistent datatypes {} and {} for variable {}",
                    s1, s2, s3
                )
            }
        }
    }
}

impl Error for OrchestrationError {}

pub async fn execute_hybrid_query(
    query: &str,
    endpoint: &str,
    time_series_database: Box<dyn TimeSeriesQueryable>,
) -> Result<DataFrame, Box<dyn Error>> {
    let parsed_query = parse_sparql_select_query(query)?;
    debug!("Parsed query: {:?}", parsed_query);
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, variable_constraints) = preprocessor.preprocess(&parsed_query);
    debug!("Constraints: {:?}", variable_constraints);
    let mut rewriter = StaticQueryRewriter::new(&variable_constraints);
    let (static_rewrite, mut time_series_queries) =
        rewriter.rewrite_query(preprocessed_query).unwrap();
    debug!("Produced static rewrite: {:?}", static_rewrite);
    let static_query_solutions = execute_sparql_query(endpoint, &static_rewrite).await?;
    complete_time_series_queries(&static_query_solutions, &mut time_series_queries)?;
    let static_result_df = create_static_query_result_df(&static_rewrite, static_query_solutions);
    debug!("Static result dataframe: {}", static_result_df);
    find_all_groupby_pushdowns(
        &parsed_query,
        &static_result_df,
        &mut time_series_queries,
        &variable_constraints,
    );
    let mut time_series =
        execute_time_series_queries(time_series_database, time_series_queries).await?;
    debug!("Time series: {:?}", time_series);
    let mut combiner = Combiner::new();
    let lazy_frame = combiner.combine_static_and_time_series_results(
        parsed_query,
        static_result_df,
        &mut time_series,
    );
    Ok(lazy_frame.collect()?)
}

fn complete_time_series_queries(
    static_query_solutions: &Vec<QuerySolution>,
    time_series_queries: &mut Vec<TimeSeriesQuery>,
) -> Result<(), OrchestrationError> {
    for tsq in time_series_queries {
        let mut ids = HashSet::new();
        if let Some(id_var) = &tsq.identifier_variable {
            for sqs in static_query_solutions {
                if let Some(Term::Literal(lit)) = sqs.get(id_var) {
                    if lit.datatype() == xsd::STRING {
                        ids.insert(lit.value().to_string());
                    } else {
                        todo!()
                    }
                }
            }
        }
        if let Some(datatype_var) = &tsq.datatype_variable {
            for sqs in static_query_solutions {
                if let Some(Term::NamedNode(nn)) = sqs.get(&datatype_var.variable) {
                    if tsq.datatype.is_none() {
                        tsq.datatype = Some(nn.clone());
                    } else if let Some(dt) = &tsq.datatype {
                        if dt.as_str() != nn.as_str() {
                            return Err(OrchestrationError::InconsistentDatatype(
                                tsq.datatype.as_ref().unwrap().as_str().to_string(),
                                dt.as_str().to_string(),
                                tsq.timeseries_variable
                                    .as_ref()
                                    .unwrap()
                                    .variable
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
        }
        let mut ids_vec: Vec<String> = ids.into_iter().collect();
        ids_vec.sort();
        tsq.ids = Some(ids_vec);
    }
    Ok(())
}

async fn execute_time_series_queries(
    mut time_series_database: Box<dyn TimeSeriesQueryable>,
    time_series_queries: Vec<TimeSeriesQuery>,
) -> Result<Vec<(TimeSeriesQuery, DataFrame)>, Box<dyn Error>> {
    let mut out = vec![];
    for tsq in time_series_queries {
        let df_res = time_series_database.execute(&tsq).await;
        match df_res {
            Ok(df) => {
                match tsq.validate(&df) {
                    Ok(_) => {}
                    Err(err) => return Err(Box::new(err)),
                }
                out.push((tsq, df))
            }
            Err(err) => return Err(err),
        }
    }
    Ok(out)
}
