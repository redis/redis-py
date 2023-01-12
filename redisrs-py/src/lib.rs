extern crate redis;

use pyo3::prelude::*;
use pyo3::{ types::{PyTuple, PyBytes, PyByteArray, PyString, PyInt, PyLong, PyFloat } };


#[pyfunction]
fn pack_command<'a>(py: Python<'a>, items: &'a PyTuple) -> &'a PyBytes {
    // Redis-py:connection.py:Encoder accepts only: bytes, string, int or float.
    // 
    let mut cmd = redis::Cmd::new().to_owned();
    for item in items {
        if item.is_instance_of::<PyByteArray>().unwrap() || item.is_instance_of::<PyBytes>().unwrap() {
            let bytes: &[u8] = item.extract().unwrap();
            cmd.arg(bytes);
        } else if item.is_instance_of::<PyString>().unwrap() {
            let str_item : String = item.extract().unwrap();
            cmd.arg(str_item);
        } else if item.is_instance_of::<PyInt>().unwrap() || item.is_instance_of::<PyLong>().unwrap() {
            let num_item : i64 = item.extract().unwrap();
            cmd.arg(num_item);
        } else if item.is_instance_of::<PyFloat>().unwrap() {
            let float_item : f64 = item.extract().unwrap();
            cmd.arg(float_item);
        } else {
            println!("not yet");
        }
    }

    PyBytes::new(py, &cmd.get_packed_command()[..])
}


#[pyfunction]
fn pack_bytes<'a>(py: Python<'a>, bytes_cmd: &'a [u8]) -> &'a PyBytes {
    
    let mut cmd = redis::Cmd::new().to_owned();
    let mut start = 0;
    for (i, &item) in bytes_cmd.iter().enumerate() {
        if item == b' ' {
            if i > start {
                cmd.arg(&bytes_cmd[start..i]);
            }
            start = i + 1;
        }
    }

    if start < bytes_cmd.len() {
        cmd.arg(&bytes_cmd[start..bytes_cmd.len()]);        
    }

    PyBytes::new(py, &cmd.get_packed_command()[..])
}


/// A Python module implemented in Rust.
#[pymodule]
fn redisrs_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(pack_command, m)?)?;
    m.add_function(wrap_pyfunction!(pack_bytes, m)?)?;
    Ok(())
}