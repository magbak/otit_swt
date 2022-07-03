#[cfg(test)]
#[macro_use] extern crate unic_char_range;
extern crate core;

extern crate chrono;
extern crate chrono_tz;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod parser;
pub mod parsing_ast;
pub mod templates;
pub mod mapping;
pub mod ast;
pub mod resolver;
mod constants;
mod ntriples_write;
pub mod document;