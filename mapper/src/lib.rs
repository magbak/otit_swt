//#[macro_use] extern crate unic_char_range; Used for generating char-ranges

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod parser;
pub mod ast;
