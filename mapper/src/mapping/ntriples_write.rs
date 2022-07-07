//Based on writer.rs: https://raw.githubusercontent.com/pola-rs/polars/master/polars/polars-io/src/csv_core/write.rs
//in Pola.rs with license:
//Copyright (c) 2020 Ritchie Vink
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
use polars::error::PolarsError;
use polars::export::rayon::iter::{IntoParallelIterator, ParallelIterator};
use polars::export::rayon::prelude::ParallelExtend;
use polars::prelude::{AnyValue, DataFrame, Series};
use polars::series::SeriesIter;
use polars_core::POOL;
use polars_utils::contention_pool::LowContentionPool;
use std::io::Write;

fn write_anyvalue(f: &mut Vec<u8>, value: AnyValue) {
    match value {
        AnyValue::Utf8(v) => write!(f, "{}", v),
        dt => panic!(
            "DataType: {} not supported, some bad change has happened",
            dt
        ),
    }
    .unwrap();
}

/// Utility to write to `&mut Vec<u8>` buffer
struct StringWrap<'a>(pub &'a mut Vec<u8>);

impl<'a> std::fmt::Write for StringWrap<'a> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

pub(crate) fn write_ntriples<W: Write + ?Sized>(
    writer: &mut W,
    df: &DataFrame,
    chunk_size: usize,
) -> Result<()> {
    let len = df.height();
    let n_threads = POOL.current_num_threads();

    let total_rows_per_pool_iter = n_threads * chunk_size;

    let any_value_iter_pool = LowContentionPool::<Vec<_>>::new(n_threads);
    let write_buffer_pool = LowContentionPool::<Vec<_>>::new(n_threads);

    let mut n_rows_finished = 0;

    // holds the buffers that will be written
    let mut result_buf = Vec::with_capacity(n_threads);
    while n_rows_finished < len {
        let par_iter = (0..n_threads).into_par_iter().map(|thread_no| {
            let thread_offset = thread_no * chunk_size;
            let total_offset = n_rows_finished + thread_offset;
            let df = df.slice(total_offset as i64, chunk_size);

            let cols = df.get_columns();

            // Safety:
            // the bck thinks the lifetime is bounded to write_buffer_pool, but at the time we return
            // the vectors the buffer pool, the series have already been removed from the buffers
            // in other words, the lifetime does not leave this scope
            let cols = unsafe { std::mem::transmute::<&Vec<Series>, &Vec<Series>>(cols) };
            let mut write_buffer = write_buffer_pool.get();

            // don't use df.empty, won't work if there are columns.
            if df.height() == 0 {
                return write_buffer;
            }

            let any_value_iters = cols.iter().map(|s| s.iter());
            let mut col_iters = any_value_iter_pool.get();
            col_iters.extend(any_value_iters);

            let last_ptr = &col_iters[col_iters.len() - 1] as *const SeriesIter;
            let mut finished = false;
            // loop rows
            while !finished {
                for col in &mut col_iters {
                    match col.next() {
                        Some(value) => {
                            write_anyvalue(&mut write_buffer, value);
                        }
                        None => {
                            finished = true;
                            break;
                        }
                    }
                    let current_ptr = col as *const SeriesIter;
                    if current_ptr != last_ptr {
                        write!(&mut write_buffer, "{}", ' ').unwrap()
                    }
                }
                if !finished {
                    writeln!(&mut write_buffer).unwrap();
                }
            }

            // return buffers to the pool
            col_iters.clear();
            any_value_iter_pool.set(col_iters);

            write_buffer
        });

        // rayon will ensure the right order
        result_buf.par_extend(par_iter);

        for mut buf in result_buf.drain(..) {
            let _ = writer.write(&buf)?;
            buf.clear();
            write_buffer_pool.set(buf);
        }

        n_rows_finished += total_rows_per_pool_iter;
    }

    Ok(())
}

pub type Result<T> = std::result::Result<T, PolarsError>;
