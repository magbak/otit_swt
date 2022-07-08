mod error;
mod to_rust;

use crate::error::PyMapperError;
use crate::to_rust::to_rust_df;
use mapper::document::document_from_str;
use mapper::errors::MapperError;
use mapper::mapping::Mapping;
use mapper::templates::TemplateDataset;
use pyo3::*;

#[pyclass]
struct PyMapping {
    inner: Mapping,
}

#[pymethods]
impl PyMapping {
    #[new]
    pub fn new(documents: Vec<&str>) -> PyResult<PyMapping> {
        let mut parsed_documents = vec![];
        for ds in documents {
            let parsed_doc = document_from_str(ds).map_err(PyMapperError::from)?;
            parsed_documents.push(parsed_doc);
        }
        let template_dataset = TemplateDataset::new(parsed_documents)
            .map_err(MapperError::from).map_err(PyMapperError::from)?;
        Ok(PyMapping {
            inner: Mapping::new(&template_dataset),
        })
    }

    pub fn expand(&mut self, template: &str, rb: &PyAny) -> PyResult<()> {
        let df = to_rust_df(&[rb])?;
        let _report = self
            .inner
            .expand(template, df, Default::default())
            .map_err(MapperError::from).map_err(PyMapperError::from)?;
        Ok(())
    }
}
