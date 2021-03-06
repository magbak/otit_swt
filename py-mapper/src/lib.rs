mod error;
mod to_rust;

use crate::error::PyMapperError;
use crate::to_rust::polars_df_to_rust_df;
use mapper::document::document_from_str;
use mapper::errors::MapperError;
use mapper::mapping::Mapping as InnerMapping;
use mapper::templates::TemplateDataset;
use pyo3::prelude::PyModule;
use pyo3::*;
use pyo3::basic::CompareOp;

#[pyclass]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct BlankNode {
    #[pyo3(get)]
    pub name: String,
}

#[pymethods]
impl BlankNode {
    fn __repr__(&self) -> String {
        format!("_:{}", self.name)
    }
}

#[pyclass]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct IRI {
    #[pyo3(get)]
    pub iri: String,
}

#[pymethods]
impl IRI {
    fn __repr__(&self) -> String {
        format!("<{}>",self.iri)
    }
}

#[pyclass]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct TripleSubject {
    #[pyo3(get)]
    pub iri: Option<IRI>,
    #[pyo3(get)]
    pub blank_node: Option<BlankNode>,
}

impl TripleSubject {
    pub fn __repr__(&self) -> String {
        if let Some(iri) = &self.iri {
            iri.__repr__()
        } else if let Some(blank_node) = &self.blank_node {
            blank_node.__repr__()
        } else {
            panic!("TripleSubject in invalid state: {:?}", self);
        }
    }
}

#[pyclass]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct Literal {
    #[pyo3(get)]
    pub lexical_form: String,
    #[pyo3(get)]
    pub language_tag: Option<String>,
    #[pyo3(get)]
    pub datatype_iri: Option<IRI>,
}

#[pymethods]
impl Literal {
    pub fn __repr__(&self) -> String {
        if let Some(tag) = &self.language_tag {
            format!("\"{}\"@{}",self.lexical_form.to_owned(), tag)
        } else if let Some(dt) = &self.datatype_iri{
            format!("\"{}\"^^{}", &self.lexical_form, dt.__repr__())
        } else {
            panic!("Literal in invalid state {:?}", self)
        }
    }
}

#[pyclass]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct TripleObject {
    #[pyo3(get)]
    pub iri: Option<IRI>,
    #[pyo3(get)]
    pub blank_node: Option<BlankNode>,
    #[pyo3(get)]
    pub literal: Option<Literal>,
}

#[pymethods]
impl TripleObject {
    pub fn __repr__(&self) -> String {
        if let Some(iri) = &self.iri {
            iri.__repr__()
        } else if let Some(blank_node) = &self.blank_node {
            blank_node.__repr__()
        } else if let Some(literal) = &self.literal {
            literal.__repr__()
        } else {
            panic!("TripleObject in invalid state: {:?}", self);
        }
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Lt => Ok(self < other),
            CompareOp::Le => Ok(self <= other),
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            CompareOp::Gt => Ok(self > other),
            CompareOp::Ge => Ok(self >= other),
        }
    }
}

#[pyclass]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct Triple {
    #[pyo3(get)]
    pub subject: TripleSubject,
    #[pyo3(get)]
    pub verb: IRI,
    #[pyo3(get)]
    pub object: TripleObject,
}

#[pymethods]
impl Triple {
    pub fn __repr__(&self) -> String {
        format!("{} {} {}", self.subject.__repr__(), self.verb.__repr__(), self.object.__repr__())
    }
}


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
            .map_err(MapperError::from)
            .map_err(PyMapperError::from)?;
        Ok(Mapping {
            inner: InnerMapping::new(&template_dataset),
        })
    }

    pub fn expand(&mut self, template: &str, df: &PyAny) -> PyResult<()> {
        let df = polars_df_to_rust_df(&df)?;
        let _report = self
            .inner
            .expand(template, df, Default::default())
            .map_err(MapperError::from)
            .map_err(PyMapperError::from)?;
        Ok(())
    }

    pub fn to_triples(&self) -> PyResult<Vec<Triple>> {
        let mut triples = vec![];

        fn create_subject(s: &str) -> TripleSubject {
            if is_blank_node(s) {
                TripleSubject {
                    iri: None,
                    blank_node: Some(BlankNode {
                        name: s.to_string(),
                    }),
                }
            } else {
                TripleSubject {
                    iri: Some(IRI { iri: s.to_string() }),
                    blank_node: None,
                }
            }
        }
        fn create_nonliteral_object(s: &str) -> TripleObject {
            if is_blank_node(s) {
                TripleObject {
                    iri: None,
                    blank_node: Some(BlankNode {
                        name: s.to_string(),
                    }),
                    literal: None,
                }
            } else {
                TripleObject {
                    iri: Some(IRI { iri: s.to_string() }),
                    blank_node: None,
                    literal: None,
                }
            }
        }
        fn create_literal(lex: &str, ltag_opt: Option<&str>, dt: &str) -> Literal {
            Literal {
                lexical_form: lex.to_string(),
                language_tag: if let Some(ltag) = ltag_opt {
                    Some(ltag.to_string())
                } else {
                    None
                },
                datatype_iri: if let Some(_) = ltag_opt {
                    None
                } else {
                    Some(IRI {iri:dt.to_string()})
                },
            }
        }

        fn to_python_object_triple(s: &str, v: &str, o: &str) -> Triple {
            let subject = create_subject(s);
            let verb = IRI { iri: v.to_string() };
            let object = create_nonliteral_object(o);
            Triple {
                subject,
                verb,
                object,
            }
        }
        fn to_python_literal_triple(
            s: &str,
            v: &str,
            lex: &str,
            ltag_opt: Option<&str>,
            dt: &str,
        ) -> Triple {
            let subject = create_subject(s);
            let verb = IRI { iri: v.to_string() };
            let literal = create_literal(lex, ltag_opt, dt);
            let object = TripleObject {
                iri: None,
                blank_node: None,
                literal: Some(literal),
            };
            Triple {
                subject,
                verb,
                object,
            }
        }
        self.inner
            .object_property_triples(to_python_object_triple, &mut triples);
        self.inner
            .data_property_triples(to_python_literal_triple, &mut triples);
        Ok(triples)
    }
}

#[pymodule]
fn otit_swt_mapper(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Mapping>()?;
    Ok(())
}

fn is_blank_node(s: &str) -> bool {
    s.starts_with("_:")
}
