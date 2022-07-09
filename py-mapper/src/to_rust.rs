// From: https://github.com/pola-rs/polars/blob/3a941854b57dd45f4ea42fe6e81af2989ba06ccf/py-polars/src/arrow_interop/to_rust.rs
// Edited to remove dependencies on py-polars
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

use polars_core::utils::arrow::array::ArrayRef;
use polars_core::utils::arrow::ffi;
use polars_core::utils::rayon::iter::{ParallelIterator, IntoParallelIterator, IndexedParallelIterator};
use polars_core::POOL;
use polars_core::prelude::{DataFrame, Series, Field, Schema, ArrowDataType};
use polars_core::utils::accumulate_dataframes_vertical;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::PyList;
use crate::error::PyMapperError;

pub fn field_to_rust(obj: &PyAny) -> PyResult<Field> {
    let schema = Box::new(ffi::ArrowSchema::empty());
    let schema_ptr = &*schema as *const ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    obj.call_method1("_export_to_c", (schema_ptr as Py_uintptr_t,))?;
    let field = unsafe { ffi::import_field_from_c(schema.as_ref()).map_err(PyMapperError::from)? };
    Ok(Field::from(&field))
}

// PyList<Field> which you get by calling `list(schema)`
pub fn pyarrow_schema_to_rust(obj: &PyList) -> PyResult<Schema> {
    obj.into_iter().map(|fld| field_to_rust(fld)).collect()
}

pub fn array_to_rust(obj: &PyAny) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(ffi::ArrowArray::empty());
    let schema = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = &*array as *const ffi::ArrowArray;
    let schema_ptr = &*schema as *const ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    obj.call_method1(
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        let field = ffi::import_field_from_c(schema.as_ref()).map_err(PyMapperError::from)?;
        let array = ffi::import_array_from_c(Box::new(*array), field.data_type).map_err(PyMapperError::from)?;
        Ok(array.into())
    }
}

pub fn polars_df_to_rust_df(df: &PyAny) -> PyResult<DataFrame> {
    let arr = df.call_method0("to_arrow")?;
    let batches = arr.call_method1("to_batches", (u32::MAX,))?;
    let batches_len = batches.call_method0("__len__")?;
    let l:u32 = batches_len.extract()?;
    assert_eq!(l, 1);
    let batch = batches.call_method1("__getitem__", (0,))?;
    array_to_rust_df(&[batch])
}

pub fn array_to_rust_df(rb: &[&PyAny]) -> PyResult<DataFrame> {
    let schema = rb
        .get(0)
        .ok_or_else(|| PyMapperError::Other("empty table".into()))?
        .getattr("schema")?;
    let names = schema.getattr("names")?.extract::<Vec<String>>()?;

    let dfs = rb
        .iter()
        .map(|rb| {
            let mut run_parallel = false;

            let columns = (0..names.len())
                .map(|i| {
                    let array = rb.call_method1("column", (i,))?;
                    let arr = array_to_rust(array)?;
                    run_parallel |= matches!(
                        arr.data_type(),
                        ArrowDataType::Utf8 | ArrowDataType::Dictionary(_, _, _)
                    );
                    Ok(arr)
                })
                .collect::<PyResult<Vec<_>>>()?;

            // we parallelize this part because we can have dtypes that are not zero copy
            // for instance utf8 -> large-utf8
            // dict encoded to categorical
            let columns = if run_parallel {
                POOL.install(|| {
                    columns
                        .into_par_iter()
                        .enumerate()
                        .map(|(i, arr)| {
                            let s = Series::try_from((names[i].as_str(), arr))
                                .map_err(PyMapperError::from)?;
                            Ok(s)
                        })
                        .collect::<PyResult<Vec<_>>>()
                })
            } else {
                columns
                    .into_iter()
                    .enumerate()
                    .map(|(i, arr)| {
                        let s = Series::try_from((names[i].as_str(), arr))
                            .map_err(PyMapperError::from)?;
                        Ok(s)
                    })
                    .collect::<PyResult<Vec<_>>>()
            }?;

            Ok(DataFrame::new(columns).map_err(PyMapperError::from)?)
        })
        .collect::<PyResult<Vec<_>>>()?;

    Ok(accumulate_dataframes_vertical(dfs).map_err(PyMapperError::from)?)
}
