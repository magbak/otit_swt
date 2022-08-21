mod resolve_columns;

use super::Mapping;
use crate::ast::{PType, Parameter, Signature};
use crate::chrono::TimeZone as ChronoTimeZone;
use crate::constants::{XSD_DATETIME_WITHOUT_TZ_FORMAT, XSD_DATETIME_WITH_TZ_FORMAT};
use crate::mapping::errors::MappingError;
use crate::mapping::ExpandOptions;
use chrono::{Datelike, Timelike};
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars_core::export::rayon::prelude::ParallelIterator;
use polars_core::frame::DataFrame;
use polars_core::prelude::{
    AnyValue, BooleanChunked, ChunkApply, DataType, IntoSeries, Series, StructChunked, TimeZone,
};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct PrimitiveColumn {
    pub rdf_node_type: RDFNodeType,
}

#[derive(Clone, Debug)]
pub enum MappedColumn {
    PrimitiveColumn(PrimitiveColumn),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RDFNodeType {
    IRI,
    BlankNode,
    Literal,
    None,
}

impl Mapping {
    pub fn find_validate_and_prepare_dataframe_columns(
        &self,
        signature: &Signature,
        mut df: DataFrame,
        options: &ExpandOptions,
    ) -> Result<(DataFrame, HashMap<String, MappedColumn>, Option<DataFrame>), MappingError> {
        let mut df_columns = HashSet::new();
        df_columns.extend(df.get_column_names().into_iter().map(|x| x.to_string()));
        let removed = df_columns.remove("Key");
        assert!(removed);

        let mut map = HashMap::new();
        let mut all_minted_iris = vec![];
        let empty_path_column_map = HashMap::new();
        let path_column_map = if let Some(m) = &options.resolve_iris {
            m
        } else {
            &empty_path_column_map
        };
        for parameter in &signature.parameter_list {
            let variable_name = &parameter.stottr_variable.name;
            if df_columns.contains(variable_name.as_str()) {
                df_columns.remove(variable_name.as_str());
                if !parameter.optional {
                    validate_non_optional_parameter(&df, variable_name)?;
                }
                if parameter.non_blank {
                    //TODO handle blanks;
                    validate_non_blank_parameter(&df, variable_name)?;
                }
                let column_data_type = infer_validate_and_prepare_column_data_type(
                    &mut df,
                    &parameter,
                    variable_name,
                    options,
                )?;

                map.insert(
                    variable_name.to_string(),
                    MappedColumn::PrimitiveColumn(column_data_type),
                );
            } else if let Some(resolve_iri) = path_column_map.get(variable_name) {
                self.resolve_iri_column(
                    resolve_iri,
                    variable_name.as_str(),
                    &mut df,
                    &mut df_columns,
                )?;
                map.insert(
                    variable_name.to_string(),
                    MappedColumn::PrimitiveColumn(PrimitiveColumn {
                        rdf_node_type: RDFNodeType::IRI,
                    }),
                );
            } else if options.mint_iris.is_some()
                && options
                    .mint_iris
                    .as_ref()
                    .unwrap()
                    .contains_key(variable_name)
            {
                let minted_iris = self.mint_iri(
                    &mut df,
                    variable_name,
                    &parameter.ptype,
                    options
                        .mint_iris
                        .as_ref()
                        .unwrap()
                        .get(variable_name)
                        .unwrap(),
                );
                all_minted_iris.push(minted_iris);
                map.insert(
                    variable_name.to_string(),
                    MappedColumn::PrimitiveColumn(PrimitiveColumn {
                        rdf_node_type: RDFNodeType::IRI,
                    }),
                );
            } else {
                return Err(MappingError::MissingParameterColumn(
                    variable_name.to_string(),
                ));
            }
        }
        if !df_columns.is_empty() {
            return Err(MappingError::ContainsIrrelevantColumns(
                df_columns.iter().map(|x| x.to_string()).collect(),
            ));
        }
        let minted_iris_df;
        if all_minted_iris.len() > 0 {
            all_minted_iris.insert(0, df.column("Key").unwrap().clone());
            minted_iris_df = Some(DataFrame::new(all_minted_iris).unwrap());
        } else {
            minted_iris_df = None;
        }
        Ok((df, map, minted_iris_df))
    }
}

fn infer_validate_and_prepare_column_data_type(
    dataframe: &mut DataFrame,
    parameter: &Parameter,
    column_name: &str,
    options: &ExpandOptions,
) -> Result<PrimitiveColumn, MappingError> {
    let series = dataframe.column(column_name).unwrap();
    let (new_series, ptype) = if let Some(ptype) = &parameter.ptype {
        (
            convert_series_if_required(series, ptype, options).unwrap(),
            ptype.clone(),
        )
    } else {
        let column_data_type = dataframe.column(column_name).unwrap().dtype().clone();
        let target_ptype = polars_datatype_to_xsd_datatype(column_data_type);
        (
            convert_series_if_required(series, &target_ptype, options).unwrap(),
            target_ptype,
        )
    };
    dataframe.with_column(new_series).unwrap();
    let rdf_node_type = infer_rdf_node_type(&ptype);
    Ok(PrimitiveColumn { rdf_node_type })
}

fn infer_rdf_node_type(ptype: &PType) -> RDFNodeType {
    match ptype {
        PType::BasicType(b) => {
            if b.as_str() == xsd::ANY_URI {
                RDFNodeType::IRI
            } else {
                RDFNodeType::Literal
            }
        }
        PType::LUBType(l) => infer_rdf_node_type(l),
        PType::ListType(l) => infer_rdf_node_type(l),
        PType::NEListType(l) => infer_rdf_node_type(l),
    }
}

fn convert_series_if_required(
    series: &Series,
    target_ptype: &PType,
    options: &ExpandOptions,
) -> Result<Series, MappingError> {
    let series_data_type = series.dtype();
    let mismatch_error = || {
        Err(MappingError::ColumnDataTypeMismatch(
            series.name().to_string(),
            series_data_type.clone(),
            target_ptype.clone(),
        ))
    };
    let convert_if_series_list = |inner| {
        if let DataType::List(_) = series_data_type {
            convert_list_series(series, inner, options)
        } else {
            mismatch_error()
        }
    };
    match target_ptype {
        PType::BasicType(bt) => {
            if let DataType::List(_) = series_data_type {
                mismatch_error()
            } else {
                Ok(convert_nonlist_series_to_value_struct_if_required(
                    series, bt, options,
                )?)
            }
        }
        PType::LUBType(inner) => convert_if_series_list(inner),
        PType::ListType(inner) => convert_if_series_list(inner),
        PType::NEListType(inner) => convert_if_series_list(inner),
    }
}

fn convert_list_series(
    series: &Series,
    inner_target_ptype: &PType,
    options: &ExpandOptions,
) -> Result<Series, MappingError> {
    let mut out = series
        .list()
        .unwrap()
        .apply(
            |x| match { convert_series_if_required(&x, inner_target_ptype, options) } {
                Ok(ser) => ser,
                Err(e) => {
                    panic!("{:?}", e)
                }
            },
        )
        .into_series();
    out.rename(series.name());
    Ok(out)
}

fn convert_nonlist_series_to_value_struct_if_required(
    series: &Series,
    nn: &NamedNode,
    options: &ExpandOptions,
) -> Result<Series, MappingError> {
    let series_data_type = series.dtype();
    let mismatch_error = || {
        MappingError::ColumnDataTypeMismatch(
            series.name().to_string(),
            series_data_type.clone(),
            PType::BasicType(nn.clone()),
        )
    };
    let mut new_series = if nn.as_str() == xsd::ANY_URI.as_str() {
        if series_data_type == &DataType::Utf8 {
            series.clone()
        } else {
            return Err(mismatch_error());
        }
    } else if nn.as_str() == xsd::BOOLEAN.as_str() {
        if series_data_type == &DataType::Boolean {
            series.cast(&DataType::Utf8).unwrap()
        } else {
            return Err(mismatch_error());
        }
    } else if nn.as_str() == xsd::UNSIGNED_INT.as_str() {
        if series_data_type == &DataType::UInt32 {
            series.cast(&DataType::Utf8).unwrap()
        } else {
            return Err(mismatch_error());
        }
    } else if nn.as_str() == xsd::UNSIGNED_LONG.as_str() {
        if series_data_type == &DataType::UInt64 {
            series.cast(&DataType::Utf8).unwrap()
        } else {
            return Err(mismatch_error());
        }
    } else if nn.as_str() == xsd::INT.as_str() {
        if series_data_type == &DataType::Int32 {
            series.cast(&DataType::Utf8).unwrap()
        } else {
            return Err(mismatch_error());
        }
    } else if nn.as_str() == xsd::LONG.as_str() {
        if series_data_type == &DataType::Int64 {
            series.cast(&DataType::Utf8).unwrap()
        } else {
            return Err(mismatch_error());
        }
    } else if nn.as_str() == xsd::FLOAT.as_str() {
        if series_data_type == &DataType::Float32 {
            series.cast(&DataType::Utf8).unwrap()
        } else {
            return Err(mismatch_error());
        }
    } else if nn.as_str() == xsd::DOUBLE.as_str() {
        if series_data_type == &DataType::Float64 {
            series.cast(&DataType::Utf8).unwrap()
        } else {
            return Err(mismatch_error());
        }
    } else if nn.as_str() == xsd::STRING.as_str() {
        if series_data_type == &DataType::Utf8 {
            series.clone()
        } else {
            return Err(mismatch_error());
        }
    } else if nn.as_str() == xsd::DATE_TIME.as_str() {
        if let DataType::Datetime(_, tz_opt) = series_data_type {
            if let Some(tz) = tz_opt {
                hack_format_timestamp_with_timezone(series, tz)?
            } else {
                series
                    .datetime()
                    .unwrap()
                    .strftime(XSD_DATETIME_WITHOUT_TZ_FORMAT)
                    .into_series()
            }
        } else {
            return Err(mismatch_error());
        }
    } else if nn.as_str() == xsd::DATE_TIME_STAMP.as_str() {
        if let DataType::Datetime(_, Some(tz)) = series_data_type {
            hack_format_timestamp_with_timezone(series, tz)?
        } else {
            return Err(mismatch_error());
        }
    } else {
        return Err(MappingError::PTypeNotSupported(
            series.name().to_string(),
            PType::BasicType(nn.clone()),
        ));
    };
    assert_eq!(new_series.dtype(), &DataType::Utf8);
    let rdf_node_type = infer_rdf_node_type(&PType::BasicType(nn.clone()));
    if rdf_node_type == RDFNodeType::Literal {
        new_series.rename("lexical_form");
        let mut language_tag = "";
        if let Some(tags) = &options.language_tags {
            if let Some(tag) = tags.get(series.name()) {
                language_tag = tag.as_str();
            }
        }
        let language_series = Series::new_empty(&"language_tag", &DataType::Utf8)
            .extend_constant(AnyValue::Utf8(language_tag), series.len())
            .unwrap();
        let data_type_series = Series::new_empty("datatype_iri", &DataType::Utf8)
            .extend_constant(AnyValue::Utf8(nn.as_str()), series.len())
            .unwrap();
        let st = StructChunked::new(
            series.name(),
            &[new_series, language_series, data_type_series],
        )
        .unwrap();
        new_series = st.into_series();
    }
    Ok(new_series)
}

fn hack_format_timestamp_with_timezone(
    series: &Series,
    tz: &TimeZone,
) -> Result<Series, MappingError> {
    let timezone_opt: Result<chrono_tz::Tz, _> = tz.parse();
    if let Ok(timezone) = timezone_opt {
        let datetime_strings = Series::from_iter(
            series
                .datetime()
                .unwrap()
                .as_datetime_iter()
                .map(|x| x.unwrap())
                .map(|x| {
                    format!(
                        "{}",
                        timezone
                            .ymd(x.year(), x.month(), x.day())
                            .and_hms_nano(x.hour(), x.minute(), x.second(), x.nanosecond())
                            .format(XSD_DATETIME_WITH_TZ_FORMAT)
                    )
                }),
        );

        Ok(datetime_strings)
    } else {
        Err(MappingError::UnknownTimeZoneError(tz.to_string()))
    }
}

fn polars_datatype_to_xsd_datatype(datatype: DataType) -> PType {
    let xsd_nn_ref = match datatype {
        DataType::Boolean => xsd::BOOLEAN,
        DataType::UInt32 => xsd::UNSIGNED_INT,
        DataType::UInt64 => xsd::UNSIGNED_LONG,
        DataType::Int32 => xsd::INT,
        DataType::Int64 => xsd::LONG,
        DataType::Float32 => xsd::FLOAT,
        DataType::Float64 => xsd::DOUBLE,
        DataType::Utf8 => xsd::STRING,
        DataType::Date => xsd::DATE,
        DataType::Datetime(_, Some(_)) => xsd::DATE_TIME_STAMP,
        DataType::Datetime(_, None) => xsd::DATE_TIME,
        DataType::Duration(_) => xsd::DURATION,
        DataType::List(inner) => {
            return PType::ListType(Box::new(polars_datatype_to_xsd_datatype(*inner)))
        }
        _ => {
            panic!("Unsupported datatype:{}", datatype)
        }
    };
    PType::BasicType(xsd_nn_ref.into_owned())
}

fn validate_non_optional_parameter(df: &DataFrame, column_name: &str) -> Result<(), MappingError> {
    if df.column(column_name).unwrap().is_null().any() {
        let is_null = df.column(column_name).unwrap().is_null();
        Err(MappingError::NonOptionalColumnHasNull(
            column_name.to_string(),
            df.column("Key").unwrap().filter(&is_null).unwrap(),
        ))
    } else {
        Ok(())
    }
}

fn validate_non_blank_parameter(df: &DataFrame, column_name: &str) -> Result<(), MappingError> {
    let is_blank_node_mask: BooleanChunked = df
        .column(column_name)
        .unwrap()
        .utf8()
        .map(move |x| {
            x.par_iter()
                .map(move |x| x.unwrap_or("").starts_with("_:"))
                .collect()
        })
        .unwrap();
    if is_blank_node_mask.any() {
        return Err(MappingError::NonBlankColumnHasBlankNode(
            column_name.to_string(),
            df.column(column_name)
                .unwrap()
                .filter(&is_blank_node_mask)
                .unwrap(),
        ));
    }
    Ok(())
}
