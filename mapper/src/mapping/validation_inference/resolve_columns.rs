use super::Mapping;
use crate::mapping::errors::MappingError;
use crate::mapping::ResolveIRI;
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
        let key_column = resolve_iri.key_column.clone();
        if !df_columns.contains(&key_column) {
            return Err(MappingError::MissingForeignKeyColumn(
                variable_name.to_string(),
                key_column,
            ));
        }
        let mut template_name = None;

        if self.minted_iris.contains_key(&resolve_iri.template) {
            template_name = Some(resolve_iri.template.clone());
        } else {
            let mut split_colon = resolve_iri.template.split(":");
            let prefix_maybe = split_colon.next();
            if let Some(prefix) = prefix_maybe {
                if let Some(nn) = self.template_dataset.prefix_map.get(prefix) {
                    let possible_template_name = nn.as_str().to_string()
                        + split_colon.collect::<Vec<&str>>().join(":").as_str();
                    if self.minted_iris.contains_key(&possible_template_name) {
                        template_name = Some(possible_template_name);
                    } else {
                        return Err(MappingError::NoMintedIRIsForTemplateNameFromPrefix(
                            possible_template_name,
                        ));
                    }
                }
            }
        }

        if template_name.is_none() {
            return Err(MappingError::NoMintedIRIsForTemplate(
                resolve_iri.template.clone(),
            ));
        }
        let use_df = self
            .minted_iris
            .get(template_name.as_ref().unwrap())
            .unwrap();

        if let Err(_) = use_df.column(&resolve_iri.argument) {
            return Err(MappingError::NoMintedIRIsForArgument(
                resolve_iri.argument.clone(),
                use_df
                    .get_column_names()
                    .into_iter()
                    .filter(|x| *x != "Key")
                    .map(|x| x.to_string())
                    .collect(),
            ));
        }

        toggle_string_cache(true);

        let mut join_df = use_df.select(["Key", &resolve_iri.argument]).unwrap();
        join_df
            .with_column(
                join_df
                    .column("Key")
                    .unwrap()
                    .cast(&DataType::Categorical(None))
                    .unwrap(),
            )
            .unwrap();

        join_df
            .rename(&resolve_iri.argument, variable_name)
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

        input_df = input_df
            .join(
                &join_df,
                [&key_column],
                [&"Key".to_string()],
                JoinType::Left,
                None,
            )
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
