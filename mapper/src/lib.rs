#[cfg(test)]
#[macro_use] extern crate unic_char_range;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod parser;
pub mod parsing_ast;
mod templates;
mod mapping;
pub mod ast;
mod resolver;
mod validation;
