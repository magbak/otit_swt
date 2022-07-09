mod error;
mod to_rust;

use crate::error::PyMapperError;
use crate::to_rust::polars_df_to_rust_df;
use mapper::document::document_from_str;
use mapper::errors::MapperError;
use mapper::mapping::Mapping as InnerMapping;
use mapper::templates::TemplateDataset;
use pyo3::*;
use pyo3::prelude::PyModule;

#[pyclass]
pub struct Mapping {
    inner: InnerMapping,
}

#[pymethods]
impl Mapping {
    #[new]
    pub fn new(documents: Vec<&str>) -> PyResult<Mapping> {
        let mut parsed_documents = vec![];
        for ds in documents {
            let parsed_doc = document_from_str(ds).map_err(PyMapperError::from)?;
            parsed_documents.push(parsed_doc);
        }
        let template_dataset = TemplateDataset::new(parsed_documents)
            .map_err(MapperError::from).map_err(PyMapperError::from)?;
        Ok(Mapping {
            inner: InnerMapping::new(&template_dataset),
        })
    }

    pub fn expand(&mut self, template: &str, df: &PyAny) -> PyResult<()> {
        let df = polars_df_to_rust_df(&df)?;
        let _report = self
            .inner
            .expand(template, df, Default::default())
            .map_err(MapperError::from).map_err(PyMapperError::from)?;
        Ok(())
    }
}

#[pymodule]
fn otit_swt_mapper(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Mapping>()?;
    Ok(())
}