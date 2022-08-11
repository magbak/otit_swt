// Adapted from: https://raw.githubusercontent.com/pola-rs/polars/master/py-polars/src/error.rs
// Original licence:
//
// Copyright (c) 2020 Ritchie Vink
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use pyo3::{
    create_exception,
    exceptions::{PyException, PyRuntimeError},
    prelude::*,
};
use std::fmt::{Debug};
use polars_core::error::{ArrowError, PolarsError};
use thiserror::Error;
use mapper::errors::MapperError;

#[derive(Error, Debug)]
pub enum PyMapperError {
    #[error(transparent)]
    MapperError(#[from] MapperError),
    #[error(transparent)]
    PolarsError(#[from] PolarsError),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

impl std::convert::From<PyMapperError> for PyErr {
    fn from(err: PyMapperError) -> PyErr {
        let default = || PyRuntimeError::new_err(format!("{:?}", &err));

        match &err {
            PyMapperError::MapperError(err) => {
                match err {
                    MapperError::IOError(i) => {IOErrorException::new_err(format!("{}", i))}
                    MapperError::ParsingError(p) => {ParsingErrorException::new_err(format!("{}", p))}
                    MapperError::ResolutionError(r) => {ResolutionErrorException::new_err(format!("{}", r))}
                    MapperError::TypingError(t) => {TypingErrorException::new_err(format!("{}", t))}
                    MapperError::MappingError(m) => {MappingErrorException::new_err(format!("{}", m))}
                }
            }
            PyMapperError::Arrow(err) => ArrowErrorException::new_err(format!("{:?}", err)),
            _ => default(),
        }
    }
}

create_exception!(exceptions, IOErrorException, PyException);
create_exception!(exceptions, ParsingErrorException, PyException);
create_exception!(exceptions, ResolutionErrorException, PyException);
create_exception!(exceptions, TypingErrorException, PyException);
create_exception!(exceptions, ArrowErrorException, PyException);
create_exception!(exceptions, MappingErrorException, PyException);