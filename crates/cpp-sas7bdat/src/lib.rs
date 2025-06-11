// src/lib.rs
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

pub mod to_polars;
pub mod read;
mod utilities;
// Include the generated bindings
#[cfg(not(rust_analyzer))]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

