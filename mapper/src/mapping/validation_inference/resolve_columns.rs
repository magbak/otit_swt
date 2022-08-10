use super::Mapping;
use crate::mapping::errors::MappingError;
use crate::mapping::{Part, ResolveIRI};
use log::warn;
use polars::prelude::SeriesOps;
use polars_core::datatypes::DataType;
use polars_core::frame::DataFrame;
use polars_core::prelude::{JoinType, NamedFrom};
use polars_core::series::Series;
use polars_core::toggle_string_cache;
use std::collections::HashSet;

impl Mapping {
    pub fn resolve_iri_column(
        &self,
        resolve_iri: &ResolveIRI,
        variable_name: &str,
        df: &mut DataFrame,
        df_columns: &mut HashSet<String>,
    ) -> Result<(), MappingError> {
        let key_column = resolve_iri.key_column_name.clone();
        if !df_columns.contains(&key_column) {
            return Err(MappingError::MissingForeignKeyColumn(
                variable_name.to_string(),
                key_column,
            ));
        }

        let mut path_series = Series::new("Path", [resolve_iri.path.clone()]);
        toggle_string_cache(true);
        path_series = path_series.cast(&DataType::Categorical(None)).unwrap();

        let use_df = match &resolve_iri.part {
            Part::Subject => {
                if let Some(df) = &self.object_property_triples {
                    if path_series
                        .is_in(
                            &df.column("Path")
                                .unwrap()
                                .cast(&DataType::Categorical(None))
                                .unwrap()
                                .unique()
                                .unwrap(),
                        )
                        .unwrap()
                        .any()
                    {
                        df
                    } else if let Some(df) = &self.data_property_triples {
                        df
                    } else {
                        panic!("Should not happen")
                    }
                } else if let Some(df) = &self.data_property_triples {
                    df
                } else {
                    panic!("Should also not happen")
                }
            }
            Part::Object => self.object_property_triples.as_ref().unwrap(),
        };

        let mut join_df = use_df
            .select(["Key", "Path", &resolve_iri.part.to_string()])
            .unwrap();
        join_df
            .with_column(
                join_df
                    .column("Path")
                    .unwrap()
                    .cast(&DataType::Categorical(None))
                    .unwrap(),
            )
            .unwrap();
        join_df
            .with_column(
                join_df
                    .column("Key")
                    .unwrap()
                    .cast(&DataType::Categorical(None))
                    .unwrap(),
            )
            .unwrap();

        join_df = join_df
            .filter(&join_df.column("Path").unwrap().is_in(&path_series).unwrap())
            .unwrap();
        join_df
            .rename(&resolve_iri.part.to_string(), variable_name)
            .unwrap();
        df.with_column(Series::new("ordering_column", 0..(df.height() as u64)))
            .unwrap();

        let use_series: Vec<Series> = df
            .columns([&key_column, "ordering_column"])
            .unwrap()
            .into_iter()
            .map(|x| x.clone())
            .collect();
        let mut input_df = DataFrame::new(use_series).unwrap();
        let mut key_col_is_list = false;
        if let DataType::List(_) = input_df.column(&key_column).unwrap().dtype() {
            input_df = unfold_list(input_df, &key_column);
            key_col_is_list = true;
        }
        input_df
            .with_column(
                input_df
                    .column(&key_column)
                    .unwrap()
                    .cast(&DataType::Categorical(None))
                    .unwrap(),
            )
            .unwrap();
        input_df.with_column(path_series).unwrap();

        input_df = input_df
            .join(
                &join_df,
                [&key_column, "Path"],
                ["Key", "Path"],
                JoinType::Left,
                None,
            )
            .unwrap()
            .drop("Path")
            .unwrap()
            .drop(&key_column)
            .unwrap();
        if key_col_is_list {
            input_df = fold_list(input_df, variable_name);
        }
        input_df = input_df.sort(&["ordering_column"], false).unwrap();

        df_columns.remove(&key_column);
        toggle_string_cache(false);

        df.with_column(input_df.column(variable_name).unwrap().clone())
            .unwrap();
        let nullsum = df.column(variable_name).unwrap().null_count();
        if nullsum > 0 {
            warn!("Path column {} has {} non-matches", variable_name, nullsum);
        }

        Ok(())
    }
}

fn unfold_list(mut df: DataFrame, column_name: &str) -> DataFrame {
    let mut found_bottom = false;
    let mut counter = 0u8;
    while !found_bottom {
        let level_name = format!("fold_list_level_{}", counter);
        df.with_column(Series::new(&level_name, 0..(df.height() as u64)))
            .unwrap();
        df = df.explode([column_name]).unwrap();
        if let DataType::List(_) = df.column(column_name).unwrap().dtype() {
            counter += 1;
        } else {
            found_bottom = true;
        }
    }
    df
}

fn fold_list(mut df: DataFrame, column_name: &str) -> DataFrame {
    let mut counters = vec![];
    for c in df.get_column_names() {
        if c.starts_with("fold_list_level_") {
            let counter: u8 = c.strip_prefix("fold_list_level_").unwrap().parse().unwrap();
            counters.push(counter)
        }
    }
    let mut counter_cols: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|x| *x != column_name)
        .map(|x| x.to_string())
        .collect();
    counters.sort();
    for counter in counters.into_iter().rev() {
        let level_name = format!("fold_list_level_{}", counter);
        df = df
            .groupby_stable(&counter_cols)
            .unwrap()
            .agg_list()
            .unwrap();
        counter_cols = counter_cols
            .into_iter()
            .filter(|x| x != &level_name)
            .collect();
        df.rename(&format!("{}_agg_list", column_name), column_name)
            .unwrap();
    }
    df
}
