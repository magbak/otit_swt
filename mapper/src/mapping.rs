use crate::ast::{
    ConstantLiteral, ConstantTerm, Instance, PType, Parameter, Signature, StottrTerm,
};
use crate::constants::{OTTR_TRIPLE, XSD_DATETIME_WITHOUT_TZ_FORMAT};
use crate::document::document_from_str;
use crate::ntriples_write::write_ntriples;
use crate::templates::TemplateDataset;
use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, Literal, NamedNode, Subject, Term, Triple};
use polars::export::rayon::iter::ParallelIterator;
use polars::lazy::prelude::{as_struct, col, concat, Expr, LiteralValue};
use polars::prelude::{
    concat_str, AnyValue, BooleanChunked, DataFrame, DataType, Field, IntoLazy, LazyFrame,
    PolarsError, Series, SeriesOps,
};
use polars::prelude::{IntoSeries, NoEq, StructChunked};
use polars::toggle_string_cache;
use polars_core::prelude::{ChunkApply, TimeZone};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::Debug;
use std::io::Write;
use std::path::Path;

pub struct Mapping {
    template_dataset: TemplateDataset,
    object_property_triples: Option<DataFrame>,
    data_property_triples: Option<DataFrame>,
}

#[derive(Debug)]
pub enum MappingErrorType {
    TemplateNotFound,
    MissingKeyColumn,
    KeyColumnDatatypeMismatch(DataType, DataType),
    KeyColumnContainsDuplicates(Series),
    KeyColumnOverlapsExisting(Series),
    NonOptionalColumnHasNull(String, Series),
    InvalidKeyColumnDataType(DataType),
    NonBlankColumnHasBlankNode(String, Series),
    MissingParameterColumn(String),
    ContainsIrrelevantColumns(Vec<String>),
    CouldNotInferStottrDatatypeForColumn(String, DataType),
    ColumnDataTypeMismatch(String, DataType, PType),
    PTypeNotSupported(String, PType),
    UnknownTimeZoneError(String),
}

#[derive(Debug)]
pub struct MappingError {
    kind: MappingErrorType,
}

pub struct MappingReport {}

#[derive(Debug, Clone)]
enum RDFNodeType {
    IRI,
    BlankNode,
    Literal,
    None,
}

#[derive(Clone, Debug)]
struct PrimitiveColumn {
    polars_datatype: DataType,
    rdf_node_type: RDFNodeType,
}

#[derive(Clone, Debug)]
enum MappedColumn {
    PrimitiveColumn(PrimitiveColumn),
}

impl Mapping {
    pub fn new(template_dataset: &TemplateDataset) -> Mapping {
        let utf8 = DataType::Utf8;
        let object_property_dataframe = DataFrame::new(vec![
            Series::new_empty("Key", &utf8),
            Series::new_empty("subject", &utf8),
            Series::new_empty("verb", &utf8),
            Series::new_empty("object", &utf8),
        ])
        .unwrap();
        let data_property_dataframe = DataFrame::new(vec![
            Series::new_empty("Key", &utf8),
            Series::new_empty("subject", &utf8),
            Series::new_empty("verb", &utf8),
            Series::new_empty("object", &DataType::Struct(literal_struct_fields())),
        ])
        .unwrap();

        Mapping {
            template_dataset: template_dataset.clone(),
            object_property_triples: Some(object_property_dataframe),
            data_property_triples: Some(data_property_dataframe),
        }
    }

    pub fn from_folder<P: AsRef<Path>>(path: P) -> Result<Mapping, Box<dyn Error>> {
        let dataset = TemplateDataset::from_folder(path)?;
        Ok(Mapping::new(&dataset))
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Mapping, Box<dyn Error>> {
        let dataset = TemplateDataset::from_file(path)?;
        Ok(Mapping::new(&dataset))
    }

    pub fn from_str(s: &str) -> Result<Mapping, Box<dyn Error>> {
        let doc = document_from_str(s.into())?;
        let dataset = TemplateDataset::new(vec![doc])?;
        Ok(Mapping::new(&dataset))
    }

    pub fn write_n_triples(&self, buffer: &mut dyn Write) -> Result<(), PolarsError> {
        //TODO: Refactor all of this stuff.. obviously poorly thought out..
        let constant_utf8_series = |s, n| {
            Expr::Literal(LiteralValue::Series(NoEq::new(
                Series::new_empty("lbrace", &DataType::Utf8)
                    .extend_constant(AnyValue::Utf8(s), n)
                    .unwrap(),
            )))
        };
        let braces_expr = |colname, n| {
            concat_str(
                [
                    constant_utf8_series("<", n),
                    col(colname),
                    constant_utf8_series(">", n),
                ],
                "",
            )
        };

        let n_object_property_triples = self.object_property_triples.as_ref().unwrap().height();
        let subject_expr = braces_expr("subject", n_object_property_triples);
        let verb_expr = braces_expr("verb", n_object_property_triples);
        let object_expr = braces_expr("object", n_object_property_triples);
        let triple_expr = concat_str(
            [
                subject_expr,
                verb_expr,
                object_expr,
                constant_utf8_series(".", n_object_property_triples),
            ],
            " ",
        );
        let objects_df = self
            .object_property_triples
            .as_ref()
            .unwrap()
            .clone()
            .lazy()
            .select(&[triple_expr.alias("")])
            .collect()
            .expect("Ok");

        let n_data_property_triples = self.data_property_triples.as_ref().unwrap().height();
        let data_subject_expr = braces_expr("subject", n_data_property_triples);
        let data_verb_expr = braces_expr("verb", n_data_property_triples);
        let data_object_expr = concat_str(
            [
                constant_utf8_series("\"", n_data_property_triples),
                col("object").struct_().field_by_name("lexical_form"),
                constant_utf8_series("\"", n_data_property_triples),
                constant_utf8_series("^^", n_data_property_triples),
                constant_utf8_series("<", n_data_property_triples),
                col("object").struct_().field_by_name("datatype_iri"),
                constant_utf8_series(">", n_data_property_triples),
            ],
            "",
        );
        let data_triple_expr = concat_str(
            [
                data_subject_expr,
                data_verb_expr,
                data_object_expr,
                constant_utf8_series(".", n_data_property_triples),
            ],
            " ",
        );
        let data_df = self
            .data_property_triples
            .as_ref()
            .unwrap()
            .clone()
            .lazy()
            .select(&[data_triple_expr.alias("")])
            .collect()
            .expect("Ok");
        let mut out_df = concat([objects_df.lazy(), data_df.lazy()], true)
            .unwrap()
            .collect()
            .unwrap();
        out_df.as_single_chunk_par();
        write_ntriples(buffer, &out_df, 1024).unwrap();
        Ok(())
    }

    pub fn to_triples(&mut self) -> Vec<Triple> {
        let mut triples = vec![];
        if let Some(object_property_triples) = &self.object_property_triples {
            if object_property_triples.height() > 0 {
                let mut subject_iterator =
                    object_property_triples.column("subject").unwrap().iter();
                let mut verb_iterator = object_property_triples.column("verb").unwrap().iter();
                let mut object_iterator = object_property_triples.column("object").unwrap().iter();

                for _ in 0..object_property_triples.height() {
                    let s = subject_iterator.next().unwrap();
                    let v = verb_iterator.next().unwrap();
                    let o = object_iterator.next().unwrap();

                    if let AnyValue::Utf8(subject) = s {
                        if let AnyValue::Utf8(verb) = v {
                            if let AnyValue::Utf8(object) = o {
                                let subject = subject_from_str(subject);
                                let verb = NamedNode::new_unchecked(verb);
                                let object = object_term_from_str(object);
                                let t = Triple::new(subject, verb, object);
                                triples.push(t);
                            } else {
                                panic!("Should never happen")
                            }
                        } else {
                            panic!("Should also never happen")
                        }
                    } else {
                        panic!("Also never")
                    }
                }
            }
        }

        if let Some(data_property_triples) = &mut self.data_property_triples {
            data_property_triples.as_single_chunk();
            if data_property_triples.height() > 0 {
                let mut subject_iterator = data_property_triples.column("subject").unwrap().iter();
                let mut verb_iterator = data_property_triples.column("verb").unwrap().iter();
                //Workaround due to not happy about struct iterator..
                let obj_col = data_property_triples.column("object").unwrap();
                let lexical_form_series = obj_col
                    .struct_()
                    .unwrap()
                    .field_by_name("lexical_form")
                    .unwrap();
                let datatype_series = obj_col
                    .struct_()
                    .unwrap()
                    .field_by_name("datatype_iri")
                    .unwrap();

                let mut lexical_iterator = lexical_form_series.iter();
                let mut datatype_iterator = datatype_series.iter();

                for _ in 0..data_property_triples.height() {
                    let s = subject_iterator.next().unwrap();
                    let v = verb_iterator.next().unwrap();
                    let l = lexical_iterator.next().unwrap();
                    let d = datatype_iterator.next().unwrap();

                    //TODO: Fix for when subject might be blank node.
                    if let AnyValue::Utf8(subject) = s {
                        if let AnyValue::Utf8(verb) = v {
                            if let AnyValue::Utf8(value) = l {
                                if let AnyValue::Utf8(datatype) = d {
                                    let subject = subject_from_str(subject);
                                    let verb = NamedNode::new_unchecked(verb);
                                    let object = Term::Literal(Literal::new_typed_literal(
                                        value,
                                        NamedNode::new_unchecked(datatype),
                                    ));
                                    let t = Triple::new(subject, verb, object);
                                    triples.push(t);
                                } else {
                                    panic!("Should never happen")
                                }
                            } else {
                                panic!("Should never happen")
                            }
                        } else {
                            panic!("Should never happen")
                        }
                    } else {
                        panic!("Should also never happen")
                    }
                }
            }
        }
        triples
    }

    pub fn expand(
        &mut self,
        name: &NamedNode,
        mut df: DataFrame,
    ) -> Result<MappingReport, MappingError> {
        self.validate_dataframe(&mut df)?;
        let target_template = self.template_dataset.get(name).unwrap();
        let columns =
            find_validate_and_prepare_dataframe_columns(&target_template.signature, &mut df)?;
        let mut result_vec = vec![];
        self._expand(name, df.lazy(), columns, &mut result_vec)?;
        self.process_results(result_vec);

        Ok(MappingReport {})
    }

    fn _expand(
        &self,
        name: &NamedNode,
        mut lf: LazyFrame,
        columns: HashMap<String, MappedColumn>,
        new_lfs_columns: &mut Vec<(LazyFrame, HashMap<String, MappedColumn>)>,
    ) -> Result<(), MappingError> {
        //At this point, the lf should have columns with names appropriate for the template to be instantiated (named_node).
        if let Some(template) = self.template_dataset.get(name) {
            if template.signature.template_name.as_str() == OTTR_TRIPLE {
                lf = lf.select(&[col("Key"), col("subject"), col("verb"), col("object")]);
                new_lfs_columns.push((lf.collect().unwrap().lazy(), columns));
                Ok(())
            } else {
                for i in &template.pattern_list {
                    let target_template = self.template_dataset.get(&i.template_name).unwrap();
                    let (instance_lf, instance_columns) = create_remapped_lazy_frame(
                        i,
                        &target_template.signature,
                        lf.clone(),
                        &columns,
                    )?;
                    self._expand(
                        &i.template_name,
                        instance_lf,
                        instance_columns,
                        new_lfs_columns,
                    )?;
                }
                Ok(())
            }
        } else {
            Err(MappingError {
                kind: MappingErrorType::TemplateNotFound,
            })
        }
    }

    fn validate_dataframe(&mut self, df: &mut DataFrame) -> Result<(), MappingError> {
        if !df.get_column_names().contains(&"Key") {
            return Err(MappingError {
                kind: MappingErrorType::MissingKeyColumn,
            });
        }
        if self
            .object_property_triples
            .as_ref()
            .unwrap()
            .get_column_names()
            .contains(&"Key")
        {
            let key_datatype = df.column("Key").unwrap().dtype().clone();
            if key_datatype != DataType::Utf8 {
                return Err(MappingError {
                    kind: MappingErrorType::InvalidKeyColumnDataType(key_datatype.clone()),
                });
            }
            if df.column("Key").unwrap().is_duplicated().unwrap().any() {
                let is_duplicated = df.column("Key").unwrap().is_duplicated().unwrap();
                let dupes = df.filter(&is_duplicated).unwrap().clone();
                return Err(MappingError {
                    kind: MappingErrorType::KeyColumnContainsDuplicates(
                        dupes.column("Key").unwrap().clone(),
                    ),
                });
            }
            toggle_string_cache(true);
            let df_keys = df
                .column("Key")
                .unwrap()
                .cast(&DataType::Categorical(None))
                .unwrap();
            let existing_keys = self
                .object_property_triples
                .as_mut()
                .unwrap()
                .column("Key")
                .unwrap()
                .cast(&DataType::Categorical(None))
                .unwrap();
            let overlapping_keys = df_keys.is_in(&existing_keys).unwrap();
            toggle_string_cache(false);

            if overlapping_keys.any() {
                return Err(MappingError {
                    kind: MappingErrorType::KeyColumnOverlapsExisting(
                        df.column("Key")
                            .unwrap()
                            .filter(&overlapping_keys)
                            .unwrap()
                            .clone(),
                    ),
                });
            }
        }
        Ok(())
    }

    fn process_results(&mut self, result_vec: Vec<(LazyFrame, HashMap<String, MappedColumn>)>) {
        let mut object_properties = vec![];
        let mut data_properties = vec![];
        for (lf, columns) in result_vec {
            let df = lf.collect().expect("Collect problem");
            match columns.get("object").unwrap() {
                MappedColumn::PrimitiveColumn(c) => match c.rdf_node_type {
                    RDFNodeType::IRI => {
                        object_properties.push(df.lazy());
                    }
                    RDFNodeType::BlankNode => {}
                    RDFNodeType::Literal => {
                        data_properties.push(df.lazy());
                    }
                    RDFNodeType::None => {}
                },
            }
        }
        let existing_object_properties = self.object_property_triples.take().unwrap();
        object_properties.push(existing_object_properties.lazy());
        self.object_property_triples = Some(
            concat(object_properties, true)
                .unwrap()
                .collect()
                .expect("Collect after concat problem"),
        );

        let existing_data_properties = self.data_property_triples.take().unwrap();
        data_properties.push(existing_data_properties.lazy());
        self.data_property_triples = Some(
            concat(data_properties, true)
                .unwrap()
                .collect()
                .expect("Collect after concat problem"),
        );
    }
}

fn find_validate_and_prepare_dataframe_columns(
    signature: &Signature,
    df: &mut DataFrame,
) -> Result<HashMap<String, MappedColumn>, MappingError> {
    let mut df_columns = HashSet::new();
    df_columns.extend(df.get_column_names().into_iter().map(|x| x.to_string()));
    let removed = df_columns.remove("Key");
    assert!(removed);

    let mut map = HashMap::new();
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
            let column_data_type =
                infer_validate_and_prepare_column_data_type(df, &parameter, variable_name)?;

            map.insert(
                variable_name.to_string(),
                MappedColumn::PrimitiveColumn(column_data_type),
            );
        } else {
            return Err(MappingError {
                kind: MappingErrorType::MissingParameterColumn(variable_name.to_string()),
            });
        }
    }
    if !df_columns.is_empty() {
        return Err(MappingError {
            kind: MappingErrorType::ContainsIrrelevantColumns(
                df_columns.iter().map(|x| x.to_string()).collect(),
            ),
        });
    }
    Ok(map)
}

fn create_remapped_lazy_frame(
    instance: &Instance,
    signature: &Signature,
    mut lf: LazyFrame,
    columns: &HashMap<String, MappedColumn>,
) -> Result<(LazyFrame, HashMap<String, MappedColumn>), MappingError> {
    let mut new_map = HashMap::new();
    let mut existing = vec![];
    let mut new = vec![];
    let mut expressions = vec![];
    for (original, target) in instance
        .argument_list
        .iter()
        .zip(signature.parameter_list.iter())
    {
        match &original.term {
            StottrTerm::Variable(v) => {
                existing.push(v.name.clone());
                new.push(target.stottr_variable.name.clone());
                new_map.insert(
                    target.stottr_variable.name.clone(),
                    columns.get(&v.name).unwrap().clone(),
                );
            }
            StottrTerm::ConstantTerm(ct) => {
                let (expr, mapped_column) = constant_to_lazy_expression(ct);
                expressions.push(expr.alias(&target.stottr_variable.name));
                new_map.insert(target.stottr_variable.name.clone(), mapped_column);
            }
            StottrTerm::List(_) => {}
        }
    }
    lf = lf.rename(existing.as_slice(), new.as_slice());
    let mut new_column_expressions: Vec<Expr> = new.into_iter().map(|x| col(&x)).collect();
    new_column_expressions.push(col("Key"));
    lf = lf.select(new_column_expressions.as_slice());
    for e in expressions {
        lf = lf.with_column(e);
    }
    Ok((lf, new_map))
}

fn infer_validate_and_prepare_column_data_type(
    dataframe: &mut DataFrame,
    parameter: &Parameter,
    column_name: &str,
) -> Result<PrimitiveColumn, MappingError> {
    let series = dataframe.column(column_name).unwrap();
    let (new_series, ptype) = if let Some(ptype) = &parameter.ptype {
        (
            convert_series_to_value_struct(series, ptype).unwrap(),
            ptype.clone(),
        )
    } else {
        let column_data_type = dataframe.column(column_name).unwrap().dtype().clone();
        let target_ptype = polars_datatype_to_xsd_datatype(column_data_type);
        (
            convert_series_to_value_struct(series, &target_ptype).unwrap(),
            target_ptype,
        )
    };
    let datatype = series.dtype().clone();
    dataframe.with_column(new_series).unwrap();
    Ok(PrimitiveColumn {
        polars_datatype: datatype,
        rdf_node_type: RDFNodeType::Literal,
    })
}

fn convert_series_to_value_struct(
    series: &Series,
    target_ptype: &PType,
) -> Result<Series, MappingError> {
    let series_data_type = series.dtype();
    let mismatch_error = || {
        Err(MappingError {
            kind: MappingErrorType::ColumnDataTypeMismatch(
                series.name().to_string(),
                series_data_type.clone(),
                target_ptype.clone(),
            ),
        })
    };
    let convert_if_series_list = |inner| {
        if let DataType::List(_) = series_data_type {
            convert_list_series(series, inner)
        } else {
            mismatch_error()
        }
    };
    match target_ptype {
        PType::BasicType(bt) => {
            if let DataType::List(_) = series_data_type {
                mismatch_error()
            } else {
                Ok(convert_nonlist_series_to_value_struct(series, bt)?)
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
) -> Result<Series, MappingError> {
    Ok(series
        .list()
        .unwrap()
        .apply(
            |x| match { convert_series_to_value_struct(&x, inner_target_ptype) } {
                Ok(ser) => ser,
                Err(e) => {
                    panic!("{:?}", e)
                }
            },
        )
        .into_series())
}

fn convert_nonlist_series_to_value_struct(
    series: &Series,
    nn: &NamedNode,
) -> Result<Series, MappingError> {
    let series_data_type = series.dtype();
    let mismatch_error = || MappingError {
        kind: MappingErrorType::ColumnDataTypeMismatch(
            series.name().to_string(),
            series_data_type.clone(),
            PType::BasicType(nn.clone()),
        ),
    };
    let mut value_series = if nn.as_str() == xsd::BOOLEAN.as_str() {
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
            series.cast(&DataType::Utf8).unwrap()
        } else {
            return Err(mismatch_error());
        }
    } else if nn.as_str() == xsd::DATE_TIME.as_str() {
        if let DataType::Datetime(unit, tz_opt) = series_data_type {
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
        if let DataType::Datetime(unit, Some(tz)) = series_data_type {
            hack_format_timestamp_with_timezone(series, tz)?
        } else {
            return Err(mismatch_error());
        }
    } else {
        return Err(MappingError {
            kind: MappingErrorType::PTypeNotSupported(
                series.name().to_string(),
                PType::BasicType(nn.clone()),
            ),
        });
    };
    assert_eq!(value_series.dtype(), &DataType::Utf8);
    value_series.rename("lexical_form");
    //TODO: Allow language to be set perhaps as an argument
    let language_series = Series::full_null(&"language_tag", series.len(), &DataType::Utf8);
    let data_type_series = Series::new_empty("datatype_iri", &DataType::Utf8)
        .extend_constant(AnyValue::Utf8(nn.as_str()), series.len())
        .unwrap();
    let st = StructChunked::new(
        series.name(),
        &[value_series, language_series, data_type_series],
    )
    .unwrap();
    let struct_value_series = st.into_series();
    Ok(struct_value_series)
}

fn hack_format_timestamp_with_timezone(
    series: &Series,
    tz: &TimeZone,
) -> Result<Series, MappingError> {
    let datetimestrings = series
        .datetime()
        .unwrap()
        .strftime(XSD_DATETIME_WITHOUT_TZ_FORMAT)
        .into_series();
    Ok(datetimestrings)
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
        Err(MappingError {
            kind: MappingErrorType::NonOptionalColumnHasNull(
                column_name.to_string(),
                df.column("Key").unwrap().filter(&is_null).unwrap(),
            ),
        })
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
        return Err(MappingError {
            kind: MappingErrorType::NonBlankColumnHasBlankNode(
                column_name.to_string(),
                df.column(column_name)
                    .unwrap()
                    .filter(&is_blank_node_mask)
                    .unwrap(),
            ),
        });
    }
    Ok(())
}

fn literal_struct_fields() -> Vec<Field> {
    vec![
        Field::new("lexical_form", DataType::Utf8),
        Field::new("language_tag", DataType::Utf8),
        Field::new("datatype_iri", DataType::Utf8),
    ]
}

fn constant_to_lazy_expression(constant_term: &ConstantTerm) -> (Expr, MappedColumn) {
    match constant_term {
        ConstantTerm::Constant(c) => match c {
            ConstantLiteral::IRI(iri) => (
                Expr::Literal(LiteralValue::Utf8(iri.as_str().to_string())),
                MappedColumn::PrimitiveColumn(PrimitiveColumn {
                    polars_datatype: DataType::Utf8,
                    rdf_node_type: RDFNodeType::IRI,
                }),
            ),
            ConstantLiteral::BlankNode(bn) => (
                Expr::Literal(LiteralValue::Utf8(bn.to_string())),
                MappedColumn::PrimitiveColumn(PrimitiveColumn {
                    polars_datatype: DataType::Utf8,
                    rdf_node_type: RDFNodeType::BlankNode,
                }),
            ),
            ConstantLiteral::Literal(lit) => {
                let struct_expr = as_struct(&[
                    Expr::Literal(LiteralValue::Utf8(lit.value.to_string())),
                    Expr::Literal(if let Some(lang) = &lit.language {
                        LiteralValue::Utf8(lang.clone())
                    } else {
                        LiteralValue::Null
                    }),
                    Expr::Literal(if let Some(dt) = &lit.data_type_iri {
                        LiteralValue::Utf8(dt.as_str().to_string())
                    } else {
                        panic!("literal in invalid state")
                    }),
                ]);
                (
                    struct_expr,
                    MappedColumn::PrimitiveColumn(PrimitiveColumn {
                        polars_datatype: DataType::Struct(literal_struct_fields()),
                        rdf_node_type: RDFNodeType::Literal,
                    }),
                )
            }
            ConstantLiteral::None => (
                Expr::Literal(LiteralValue::Null),
                MappedColumn::PrimitiveColumn(PrimitiveColumn {
                    polars_datatype: DataType::Null,
                    rdf_node_type: RDFNodeType::None,
                }),
            ),
        },
        ConstantTerm::ConstantList(cl) => {
            todo!()
        }
    }
}

fn subject_from_str(s: &str) -> Subject {
    if is_blank_node(s) {
        Subject::BlankNode(BlankNode::new_unchecked(s))
    } else {
        Subject::NamedNode(NamedNode::new_unchecked(s))
    }
}

fn object_term_from_str(s: &str) -> Term {
    if is_blank_node(s) {
        Term::BlankNode(BlankNode::new_unchecked(s))
    } else {
        Term::NamedNode(NamedNode::new_unchecked(s))
    }
}

fn is_blank_node(s: &str) -> bool {
    s.starts_with("_:")
}
