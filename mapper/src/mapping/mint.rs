use super::Mapping;
use crate::ast::PType;
use crate::mapping::{ListLength, MintingOptions, SuffixGenerator};
use log::warn;
use polars_core::prelude::{AnyValue, DataFrame, DataType, Series};
use uuid::Uuid;

impl Mapping {
    pub(crate) fn mint_iri(
        &self,
        df: &mut DataFrame,
        variable_name: &str,
        ptype_opt: &Option<PType>,
        minting_options: &MintingOptions,
    ) -> Series {
        assert!(!df.get_column_names().contains(&variable_name));
        let n_start = match minting_options.suffix_generator {
            SuffixGenerator::Numbering(numbering) => numbering,
        };

        let prefix = &minting_options.prefix;

        let is_list = if let Some(ptype) = ptype_opt {
            match ptype {
                PType::BasicType(_) => false,
                PType::LUBType(_) => true,
                PType::ListType(_) => true,
                PType::NEListType(_) => true,
            }
        } else {
            false
        };
        let series = if let Some(ll) = &minting_options.list_length {
            if !is_list {
                warn!(
                    "Consider annotating the variable {} as a list",
                    variable_name
                )
            }

            match ll {
                ListLength::Constant(c) => {
                    let mut dummy_series = Series::new_empty("dummy", &DataType::Null);
                    dummy_series = dummy_series
                        .extend_constant(
                            AnyValue::List(Series::full_null("dummy", *c, &DataType::Null)),
                            df.height(),
                        )
                        .unwrap();
                    mint_iri_series_same_as_column(&dummy_series, variable_name, n_start, prefix)
                }
                ListLength::SameAsColumn(c) => mint_iri_series_same_as_column(
                    df.column(c).unwrap(),
                    variable_name,
                    n_start,
                    prefix,
                ),
            }
        } else {
            mint_iri_numbering(variable_name, n_start, df.height(), prefix)
        };
        let out_series = series.clone();
        df.with_column(series).unwrap();
        out_series
    }
}

fn mint_iri_series_same_as_column(
    same_as: &Series,
    variable_name: &str,
    n_start: usize,
    prefix: &str,
) -> Series {
    if let DataType::List(_) = same_as.dtype() {
        let mut df = DataFrame::new(vec![same_as.clone()]).unwrap();
        let mut inner_list = true;
        let mut col_names = vec![];
        while inner_list {
            let row_num_name = Uuid::new_v4().to_string();
            df = df.with_row_count(&row_num_name, None).unwrap();
            df = df.explode([same_as.name()]).unwrap();
            col_names.push(row_num_name);
            if let DataType::List(_) = df.column(same_as.name()).unwrap().dtype() {
                //More explosions needed
            } else {
                inner_list = false;
            }
        }
        df.with_column(mint_iri_numbering(
            variable_name,
            n_start,
            df.height(),
            prefix,
        ))
        .unwrap();
        for i in 0..col_names.len() {
            df = df
                .groupby(&col_names[0..(col_names.len() - i)])
                .unwrap()
                .agg_list()
                .unwrap();
            df.rename(&format!("{}_agg_list", variable_name), variable_name)
                .unwrap();
        }
        df.column(variable_name).unwrap().clone()
    } else {
        panic!("Should not be called with non-list series");
    }
}

fn mint_iri_numbering(variable_name: &str, n_start: usize, length: usize, prefix: &str) -> Series {
    let new_n_start = n_start + length;
    let iri_fun = |i| format!("{}{}", prefix, i);
    let mut iri_series = Series::from_iter((n_start..new_n_start).map(iri_fun));
    iri_series.rename(variable_name);
    iri_series
}
