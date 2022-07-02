use crate::ast::{
    ConstantLiteral, ConstantTerm, Instance, PType, Parameter, Signature, StottrTerm,
};
use crate::constants::OTTR_TRIPLE;
use crate::document::document_from_str;
use crate::ntriples_write::write_ntriples;
use crate::templates::TemplateDataset;
use log::warn;
use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode, Subject, Term, Triple};
use polars::export::rayon::iter::ParallelIterator;
use polars::lazy::prelude::{as_struct, col, concat, Expr, LiteralValue};
use polars::prelude::{
    concat_str, AnyValue, BooleanChunked, DataFrame, DataType, Field, IntoLazy, LazyFrame,
    PolarsError, Series,
};
use polars::prelude::{IntoSeries, NoEq, StructChunked};
use polars::toggle_string_cache;
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
        let cat = DataType::Categorical(None);
        let object_property_dataframe = DataFrame::new(vec![
            Series::new_empty("Key", &cat),
            Series::new_empty("subject", &cat),
            Series::new_empty("verb", &cat),
            Series::new_empty("object", &cat),
        ])
        .unwrap();
        let data_property_dataframe = DataFrame::new(vec![
            Series::new_empty("Key", &cat),
            Series::new_empty("subject", &cat),
            Series::new_empty("verb", &cat),
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

                for i in 0..object_property_triples.height() {
                    let s = subject_iterator.next().unwrap();
                    let v = verb_iterator.next().unwrap();
                    let o = object_iterator.next().unwrap();
                    if let AnyValue::Categorical(u_s, r_s) = s {
                        if let AnyValue::Categorical(u_v, r_v) = v {
                            if let AnyValue::Categorical(u_o, r_o) = o {
                                let subject = NamedNode::new_unchecked(r_s.get(u_s));
                                let verb = NamedNode::new_unchecked(r_v.get(u_v));
                                let object = NamedNode::new_unchecked(r_o.get(u_o));
                                let t = Triple::new(
                                    Subject::NamedNode(subject),
                                    verb,
                                    Term::NamedNode(object),
                                );
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

                for i in 0..data_property_triples.height() {
                    let s = subject_iterator.next().unwrap();
                    let v = verb_iterator.next().unwrap();
                    let l = lexical_iterator.next().unwrap();
                    let d = datatype_iterator.next().unwrap();

                    if let AnyValue::Categorical(u_s, r_s) = s {
                        if let AnyValue::Categorical(u_v, r_v) = v {
                            if let AnyValue::Utf8(value) = l {
                                if let AnyValue::Categorical(u_d, r_d) = d {
                                    let subject = NamedNode::new_unchecked(r_s.get(u_s));
                                    let verb = NamedNode::new_unchecked(r_v.get(u_v));
                                    let object = Term::Literal(Literal::new_typed_literal(
                                        value,
                                        NamedNode::new_unchecked(r_d.get(u_d)),
                                    ));
                                    let t = Triple::new(Subject::NamedNode(subject), verb, object);
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
                new_lfs_columns.push((lf, columns));
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
                .cast(&DataType::Utf8)
                .unwrap()
                .cast(&DataType::Categorical(None))
                .unwrap();
            let overlapping_keys = df_keys.is_in(&existing_keys).unwrap();
            toggle_string_cache(false);

            df.with_column(
                df.column("Key")
                    .unwrap()
                    .cast(&DataType::Categorical(None))
                    .unwrap(),
            )
            .unwrap();

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
    let column_data_type = dataframe.column(column_name).unwrap().dtype().clone();
    let out_column = if let Some(ptype) = &parameter.ptype {
        match ptype {
            PType::BasicType(bt) => {
                if xsd::ANY_URI.as_str() == bt.as_str() {
                    if column_data_type == DataType::Utf8 {
                        convert_utf8_to_categorical(dataframe, column_name);
                        PrimitiveColumn {
                            //TODO: make a mapping..
                            polars_datatype: DataType::Categorical(None),
                            rdf_node_type: RDFNodeType::IRI,
                        }
                    } else {
                        return Err(MappingError {
                            kind: MappingErrorType::ColumnDataTypeMismatch(
                                column_name.to_string(),
                                column_data_type,
                                ptype.clone(),
                            ),
                        });
                    }
                } else {
                    todo!()
                }
            }
            PType::LUBType(_) => {
                todo!()
            }
            PType::ListType(_) => {
                todo!()
            }
            PType::NEListType(_) => {
                todo!()
            }
        }
    } else {
        // No ptype!
        if column_data_type == DataType::Utf8 {
            warn!(
                "Could not infer type for column {}, assuming it is an IRI-column.",
                column_name
            );
            convert_utf8_to_categorical(dataframe, column_name);
            PrimitiveColumn {
                //TODO: make a mapping..
                polars_datatype: DataType::Categorical(None),
                rdf_node_type: RDFNodeType::IRI,
            }
        } else if column_data_type == DataType::Null {
            PrimitiveColumn {
                polars_datatype: DataType::Null,
                rdf_node_type: RDFNodeType::None,
            }
        } else if column_data_type == DataType::Int32 {
            convert_column_to_value_struct(dataframe, column_name, &xsd::INTEGER.into_owned());
            PrimitiveColumn {
                polars_datatype: DataType::Int32,
                rdf_node_type: RDFNodeType::Literal,
            }
        } else {
            return Err(MappingError {
                kind: MappingErrorType::CouldNotInferStottrDatatypeForColumn(
                    column_name.to_string(),
                    column_data_type,
                ),
            });
        }
    };
    Ok(out_column)
}

fn convert_column_to_value_struct(
    dataframe: &mut DataFrame,
    column_name: &str,
    swt_data_type: &NamedNode,
) {
    let mut value_series = dataframe
        .column(column_name)
        .unwrap()
        .cast(&DataType::Utf8)
        .unwrap();
    value_series.rename("lexical_form");
    //TODO: Allow language to be set perhaps as an argument
    let language_series = Series::full_null(&"language_tag", dataframe.height(), &DataType::Utf8)
        .cast(&DataType::Categorical(None))
        .unwrap();
    let data_type_series = Series::new_empty("datatype_iri", &DataType::Utf8)
        .extend_constant(AnyValue::Utf8(swt_data_type.as_str()), dataframe.height())
        .unwrap()
        .cast(&DataType::Categorical(None))
        .unwrap();
    let st = StructChunked::new(
        column_name,
        &[value_series, language_series, data_type_series],
    )
    .unwrap();
    let struct_value_series = st.into_series();

    dataframe.with_column(struct_value_series).unwrap();
}

fn convert_utf8_to_categorical(dataframe: &mut DataFrame, column_name: &str) {
    dataframe
        .with_column(
            dataframe
                .column(column_name)
                .unwrap()
                .cast(&DataType::Categorical(None))
                .unwrap(),
        )
        .unwrap();
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
        Field::new("language_tag", DataType::Categorical(None)),
        Field::new("datatype_iri", DataType::Categorical(None)),
    ]
}

fn constant_to_lazy_expression(constant_term: &ConstantTerm) -> (Expr, MappedColumn) {
    match constant_term {
        ConstantTerm::Constant(c) => match c {
            ConstantLiteral::IRI(iri) => (
                Expr::Literal(LiteralValue::Utf8(iri.as_str().to_string()))
                    .cast(DataType::Categorical(None)),
                MappedColumn::PrimitiveColumn(PrimitiveColumn {
                    polars_datatype: DataType::Categorical(None),
                    rdf_node_type: RDFNodeType::IRI,
                }),
            ),
            ConstantLiteral::BlankNode(bn) => (
                Expr::Literal(LiteralValue::Utf8(bn.to_string())),
                MappedColumn::PrimitiveColumn(PrimitiveColumn {
                    polars_datatype: DataType::Categorical(None),
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
                    })
                    .cast(DataType::Categorical(None)),
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
