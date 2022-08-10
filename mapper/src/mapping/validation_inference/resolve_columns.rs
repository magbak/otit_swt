use crate::mapping::errors::MappingError;
use crate::mapping::{Part, PathColumn};
use polars_core::frame::DataFrame;
use std::collections::HashSet;
use nom::Parser;
use polars::prelude::range;
use polars_core::datatypes::DataType;
use polars_core::prelude::{JoinType, NamedFrom};
use polars_core::series::Series;
use polars_core::toggle_string_cache;
use super::Mapping;

impl Mapping {
    pub fn resolve_path_key_column(
        &self,
        path_column: &PathColumn,
        variable_name: &str,
        df: &mut DataFrame,
        df_columns: &mut HashSet<String>,
    ) -> Result<(), MappingError> {
        let key_column = format!("{}ForeignKey", variable_name.as_str());
        if !df_columns.contains(&key_column) {
            return Err(MappingError::MissingForeignKeyColumn(
                variable_name.as_str().to_string(),
                key_column,
            ));
        }

        let mut path_series = Series::new("Path", [path_column.path.clone()]);
        toggle_string_cache(true);
        path_series = path_series.cast(&DataType::Categorical(None)).unwrap();

        let use_df = match &path_column.part {
            Part::Subject => {
                if let Some(df) = &self.object_property_triples {
                    if path_series
                        .is_in(
                            &df.column(variable_name)
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
            .select(["Key", "Path", &path_column.part.to_string()])
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
            .rename(&path_column.part.to_string(), variable_name)
            .unwrap();

        let mut input_df = DataFrame::new(df.columns([&key_column]).unwrap()).unwrap();
        let mut key_col_is_list = false;
        if let DataType::List(_) = input_df.column(&key_column).unwrap().dtype() {
            input_df = unfold_list(input_df, &key_column);
            key_col_is_list = true;
        }
        input_df.with_column(
        input_df.column(&key_column)
            .unwrap()
            .cast(&DataType::Categorical(None))
            .unwrap())
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
            .unwrap();
        if key_col_is_list {
            input_df = fold_list(input_df, variable_name);
        }

        df_columns.remove(&key_column);
        toggle_string_cache(false);


        df.with_column(df.column("Key").unwrap().cast(&DataType::Utf8).unwrap())
            .unwrap();
        df.with_column(
            df.column(variable_name)
                .unwrap()
                .cast(&DataType::Utf8)
                .unwrap(),
        )
            .unwrap();
        let nullsum = df.column(variable_name).unwrap().null_count();
        if nullsum > 0 {
            warn!("Path column {} has {} non-matches", variable_name, nullsum);
        }

        Ok(())
    }
}

fn unfold_list(mut df: DataFrame, column_name: &str) {
    let mut found_bottom = false;
    let mut counter = 0u8;
    while !found_bottom {
        df.with_column(Series::new(&level_name, 0..(df.height() as u64))).unwrap();
        df = df.explode([column_name]).unwrap();
    }
}

fn fold_list(mut df: DataFrame, column_name: &str) -> DataFrame {
    let mut counters = vec![];
    for c in df.get_column_names() {
        if c.starts_with("fold_list_level_") {
            let counter:u8 = c.strip_prefix("fold_list_level_").unwrap().parse().unwrap();
            counters.push(counter)
        }
    }
    level_cols.sort_by_key(|x|-x);
    for counter in level_cols {
        let level_name = format!("fold_list_level_{}", counter);
        df = DataFrame::new(df.columns([&level_name, column_name]).unwrap()).unwrap().groupby_stable([&level_name]).unwrap().agg_list().unwrap();
        df.rename(&format!("{}_list", column_name), column_name).unwrap();
    }

    let mut found_bottom = false;
    let mut counter = 0u8;
    while !found_bottom {
        let level_name = format!("fold_list_level_{}", counter);
        df.with_column(Series::new(&level_name, 0..(df.height() as u64))).unwrap();
        df = df.explode([column_name]).unwrap();
    }
}
