#[cfg(test)]
#[macro_use]
extern crate unic_char_range;
extern crate core;

extern crate chrono;
extern crate chrono_tz;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod ast;
mod constants;
mod ntriples_write;
mod parser;
mod parsing_ast;
mod resolver;

pub mod templates;
pub mod document;
pub mod mapping;
mod parser_test;