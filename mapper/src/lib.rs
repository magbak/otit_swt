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
mod resolver;
mod parsing;

pub mod templates;
pub mod document;
pub mod mapping;

