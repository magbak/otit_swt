use crate::combiner::Combiner;
use crate::preprocessing::Preprocessor;
use crate::rewriting::StaticQueryRewriter;
use crate::splitter::parse_sparql_select_query;
use crate::static_sparql::execute_sparql_query;
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::vocab::xsd;
use oxrdf::Term;
use polars::frame::DataFrame;
use sparesults::QuerySolution;
use std::error::Error;

pub async fn execute_hybrid_query(
    query: &str,
    endpoint: &str,
    time_series_database: Box<dyn TimeSeriesQueryable>,
) -> Result<DataFrame, Box<dyn Error>> {
    let parsed_query = parse_sparql_select_query(query)?;
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed_query);
    let mut rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrite, mut time_series_queries) =
        rewriter.rewrite_query(preprocessed_query).unwrap();
    let static_query_solutions = execute_sparql_query(endpoint, static_rewrite).await?;
    complete_time_series_queries(&static_query_solutions, &mut time_series_queries);
    let mut time_series = execute_time_series_queries(time_series_database, time_series_queries)?;
    let mut combiner = Combiner::new();
    let lazy_frame = combiner.combine_static_and_time_series_results(parsed_query, static_query_solutions, &mut time_series);
    Ok(lazy_frame.collect()?)
}

fn complete_time_series_queries(
    static_query_solutions: &Vec<QuerySolution>,
    time_series_queries: &mut Vec<TimeSeriesQuery>,
) {
    for tsq in time_series_queries {
        let mut ids = vec![];
        if let Some(id_var) = &tsq.identifier_variable {
            for sqs in static_query_solutions {
                if let Some(Term::Literal(lit)) = sqs.get(id_var) {
                    if lit.datatype() == xsd::STRING {
                        ids.push(lit.value().to_string());
                    } else {
                        todo!()
                    }
                }
            }
        }
        tsq.ids = Some(ids);
    }
}

fn execute_time_series_queries(
    time_series_database: Box<dyn TimeSeriesQueryable>,
    time_series_queries: Vec<TimeSeriesQuery>,
) -> Result<Vec<(TimeSeriesQuery, DataFrame)>, Box<dyn Error>> {
    let mut out = vec![];
    for tsq in time_series_queries {
        let df_res = time_series_database.execute(&tsq);
        match df_res {
            Ok(df) => out.push((tsq, df)),
            Err(err) => return Err(err),
        }
    }
    Ok(out)
}
