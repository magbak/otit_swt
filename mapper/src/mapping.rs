use crate::ast::{
    Argument, ConstantLiteral, ConstantTerm, Instance, ListExpanderType, PType, Parameter,
    Signature, StottrTerm,
};
use crate::constants::OTTR_TRIPLE;
use crate::templates::TemplateDataset;
use log::warn;
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars::export::rayon::iter::ParallelIterator;
use polars::lazy::prelude::{as_struct, col, concat, Expr, LiteralValue};
use polars::prelude::{BooleanChunked, DataFrame, DataType, Field, IntoLazy, LazyFrame, Series};
use polars::toggle_string_cache;
use std::collections::{HashMap, HashSet};

pub struct Mapping {
    template_dataset: TemplateDataset,
    object_property_triples: DataFrame,
    data_property_triples: DataFrame,
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
    ColumnDataTypeMismatch(String, DataType, PType)
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
        toggle_string_cache(true);
        Mapping {
            template_dataset: template_dataset.clone(),
            object_property_triples: Default::default(),
            data_property_triples: Default::default(),
        }
    }

    pub fn expand(
        &mut self,
        name: &NamedNode,
        mut df: DataFrame,
    ) -> Result<MappingReport, MappingError> {
        self.validate_dataframe(&df)?;
        println!("{}", name);
        let target_template = self.template_dataset.get(name).unwrap();
        let columns =
            find_validate_and_prepare_dataframe_columns(&target_template.signature, &mut df)?;
        let lf = self._expand(name, df.lazy(), columns)?;
        println!("{}", lf.collect().expect("DF collect problem"));
        Ok(MappingReport {})
    }

    fn _expand(
        &self,
        name: &NamedNode,
        lf: LazyFrame,
        columns: HashMap<String, MappedColumn>,
    ) -> Result<LazyFrame, MappingError> {
        //At this point, the lf should have columns with names appropriate for the template to be instantiated (named_node).
        if let Some(template) = self.template_dataset.get(name) {
            if template.signature.template_name.as_str() == OTTR_TRIPLE {
                Ok(lf)
            } else {
                let mut lfs = vec![];
                for i in &template.pattern_list {
                    let target_template = self.template_dataset.get(&i.template_name).unwrap();
                    let (instance_lf, instance_columns) = create_remapped_lazy_frame(
                        i,
                        &target_template.signature,
                        lf.clone(),
                        &columns,
                    )?;
                    lfs.push(self._expand(&i.template_name, instance_lf, instance_columns)?);
                }
                Ok(concat(lfs, false).expect("Concat problem"))
            }
        } else {
            Err(MappingError {
                kind: MappingErrorType::TemplateNotFound,
            })
        }
    }

    fn validate_dataframe(&self, df: &DataFrame) -> Result<(), MappingError> {
        if !df.get_column_names().contains(&"Key") {
            return Err(MappingError {
                kind: MappingErrorType::MissingKeyColumn,
            });
        }
        if self
            .object_property_triples
            .get_column_names()
            .contains(&"Key")
        {
            let existing_key_datatype = self.object_property_triples.column("Key").unwrap().dtype();
            if !(existing_key_datatype == &DataType::Utf8
                || existing_key_datatype == &DataType::Categorical(None)
                || existing_key_datatype == &DataType::UInt32
                || existing_key_datatype == &DataType::UInt64)
            {
                return Err(MappingError {
                    kind: MappingErrorType::InvalidKeyColumnDataType(existing_key_datatype.clone()),
                });
            }

            let new_key_datatype = df.column("Key").unwrap().dtype();
            if !(new_key_datatype == &DataType::Utf8
                && existing_key_datatype == &DataType::Categorical(None))
                && new_key_datatype != existing_key_datatype
            {
                return Err(MappingError {
                    kind: MappingErrorType::KeyColumnDatatypeMismatch(
                        new_key_datatype.clone(),
                        existing_key_datatype.clone(),
                    ),
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
            let existing_keys = self.object_property_triples.column("Key").unwrap();
            let overlapping_keys = df.column("Key").unwrap().is_in(existing_keys).unwrap();
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
                todo!()
            }
            StottrTerm::List(_) => {}
        }
    }
    lf = lf.rename(existing.as_slice(), new.as_slice());
    let mut new_column_expressions: Vec<Expr> = new.into_iter().map(|x| col(&x)).collect();
    new_column_expressions.push(col("Key"));
    lf = lf.select(new_column_expressions.as_slice());
    Ok((lf, new_map))
}

fn infer_validate_and_prepare_column_data_type(
    dataframe: &mut DataFrame,
    parameter: &Parameter,
    column_name: &str,
) -> Result<PrimitiveColumn, MappingError> {
    let column_data_type = dataframe.column(column_name).unwrap().dtype().clone();
    Ok(if let Some(ptype) = &parameter.ptype {
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
                        return Err(MappingError{kind:MappingErrorType::ColumnDataTypeMismatch(column_name.to_string(), column_data_type, ptype.clone())});
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
            //TODO: dataframe.with_column(dataframe.column(column_name).unwrap())
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
    })
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
        Field::new("language_tag", DataType::Utf8),
        Field::new("datatype_iri", DataType::Categorical(None)),
    ]
}

fn constant_to_lazy_expression(constant_term: &ConstantTerm) -> (Expr, MappedColumn) {
    match constant_term {
        ConstantTerm::Constant(c) => match c {
            ConstantLiteral::IRI(iri) => (
                Expr::Literal(LiteralValue::Utf8(iri.to_string())),
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
