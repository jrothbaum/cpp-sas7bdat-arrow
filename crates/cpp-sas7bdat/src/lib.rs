// src/lib.rs
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

// Include the generated bindings
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[derive(Debug, Clone)]
pub enum SasValue {
    String(String),
    Numeric(f64),
    Date(i32),      // Days since epoch
    DateTime(i64),  // Microseconds since epoch  
    Time(i64),      // Nanoseconds since midnight
    Null,
}

#[derive(Debug, Clone)]
pub struct SasColumn {
    pub name: String,
    pub column_type: SasColumnType,
    pub length: usize,
}

#[derive(Debug, Clone)]
pub enum SasColumnType {
    String,
    Numeric,
    Date,
    DateTime,
    Time,
}

pub struct SasChunkedReader {
    handle: ChunkedReaderHandle,
    properties: Vec<SasColumn>,
}

impl SasChunkedReader {
    pub fn new(filename: &str, chunk_size: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let c_filename = CString::new(filename)?;
        let handle = unsafe { chunked_reader_create(c_filename.as_ptr(), chunk_size) };
        
        if handle.is_null() {
            return Err("Failed to create chunked reader".into());
        }
        
        // Get properties
        let mut c_properties = CProperties {
            columns: ptr::null_mut(),
            column_count: 0,
            total_rows: 0,
        };
        
        let result = unsafe { chunked_reader_get_properties(handle, &mut c_properties) };
        if result != 0 {
            unsafe { chunked_reader_destroy(handle) };
            return Err("Failed to get properties".into());
        }
        
        // Convert properties to Rust
        let properties = unsafe {
            let slice = std::slice::from_raw_parts(c_properties.columns, c_properties.column_count);
            slice.iter().map(|c_col| {
                let name = CStr::from_ptr(c_col.name).to_string_lossy().into_owned();
                let column_type = match c_col.column_type {
                    0 => SasColumnType::String,
                    1 => SasColumnType::Numeric,
                    2 => SasColumnType::Date,
                    3 => SasColumnType::DateTime,
                    4 => SasColumnType::Time,
                    _ => SasColumnType::String,
                };
                SasColumn {
                    name,
                    column_type,
                    length: c_col.length,
                }
            }).collect()
        };
        
        // Free C properties
        unsafe { free_properties(&mut c_properties) };
        
        Ok(SasChunkedReader { handle, properties })
    }
    
    pub fn properties(&self) -> &[SasColumn] {
        &self.properties
    }
    
    pub fn chunk_iterator(&mut self) -> Result<Option<ChunkIterator>, Box<dyn std::error::Error>> {
        let mut chunk_info = CChunkInfo {
            row_count: 0,
            start_row: 0,
            end_row: 0,
        };
        
        let result = unsafe { chunked_reader_next_chunk(self.handle, &mut chunk_info) };
        
        if result != 0 {
            return Ok(None); // No more chunks
        }
        
        let iterator_handle = unsafe { chunk_iterator_create(self.handle) };
        
        if iterator_handle.is_null() {
            return Err("Failed to create chunk iterator".into());
        }
        
        Ok(Some(ChunkIterator {
            handle: iterator_handle,
            row_count: chunk_info.row_count,
            current_row: 0,
            columns: self.properties.clone(),
        }))
    }
    
    pub fn has_more_chunks(&self) -> bool {
        unsafe { chunked_reader_has_chunk(self.handle) != 0 }
    }
}

impl Drop for SasChunkedReader {
    fn drop(&mut self) {
        unsafe { chunked_reader_destroy(self.handle) };
    }
}

pub struct ChunkIterator {
    handle: ChunkIteratorHandle,
    row_count: usize,
    current_row: usize,
    columns: Vec<SasColumn>,
}

impl Iterator for ChunkIterator {
    type Item = Result<Vec<SasValue>, Box<dyn std::error::Error>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row >= self.row_count {
            return None;
        }
        
        let has_next = unsafe { chunk_iterator_has_next(self.handle) };
        if has_next == 0 {
            return None;
        }
        
        let mut row_data = CRowData {
            values: ptr::null_mut(),
            column_count: 0,
        };
        
        let result = unsafe { chunk_iterator_next_row(self.handle, &mut row_data) };
        
        if result != 0 {
            return Some(Err("Failed to read next row".into()));
        }
        
        // Convert C row data to Rust
        let rust_row = unsafe {
            let slice = std::slice::from_raw_parts(row_data.values, row_data.column_count);
            let mut row = Vec::with_capacity(row_data.column_count);
            
            for (i, &c_val) in slice.iter().enumerate() {
                let value = if c_val.is_null != 0 {
                    SasValue::Null
                } else {
                    match c_val.value_type {
                        1 => {
                            let c_str = CStr::from_ptr(c_val.string_val);
                            SasValue::String(c_str.to_string_lossy().into_owned())
                        }
                        2 => {
                            // Check column type for proper conversion
                            match self.columns[i].column_type {
                                SasColumnType::Date => {
                                    // Convert SAS date to days since Unix epoch
                                    let days = c_val.numeric_val as i32 - 3653;
                                    SasValue::Date(days)
                                }
                                SasColumnType::DateTime => {
                                    // Convert SAS datetime to microseconds since Unix epoch
                                    let microseconds = ((c_val.numeric_val - 315619200.0) * 1_000_000.0) as i64;
                                    SasValue::DateTime(microseconds)
                                }
                                SasColumnType::Time => {
                                    // Convert SAS time to nanoseconds since midnight
                                    let nanoseconds = (c_val.numeric_val * 1_000_000_000.0) as i64;
                                    SasValue::Time(nanoseconds)
                                }
                                _ => SasValue::Numeric(c_val.numeric_val),
                            }
                        }
                        _ => SasValue::Null,
                    }
                };
                row.push(value);
            }
            
            row
        };
        
        // Clean up C memory
        unsafe { free_row_data(&mut row_data) };
        
        self.current_row += 1;
        Some(Ok(rust_row))
    }
    
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.row_count - self.current_row;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for ChunkIterator {}

impl Drop for ChunkIterator {
    fn drop(&mut self) {
        unsafe { chunk_iterator_destroy(self.handle) };
    }
}

// Optional Arrow/Polars integration
#[cfg(feature = "arrow")]
pub mod arrow_integration {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    
    impl ChunkIterator {
        pub fn to_arrow_batch(&mut self) -> Result<RecordBatch, Box<dyn std::error::Error>> {
            let mut rows = Vec::new();
            for row_result in self {
                rows.push(row_result?);
            }
            
            if rows.is_empty() {
                return Err("No rows to convert".into());
            }
            
            // Build Arrow schema
            let fields: Vec<Field> = self.columns.iter().map(|col| {
                let data_type = match col.column_type {
                    SasColumnType::String => DataType::Utf8,
                    SasColumnType::Numeric => DataType::Float64,
                    SasColumnType::Date => DataType::Date32,
                    SasColumnType::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
                    SasColumnType::Time => DataType::Time64(TimeUnit::Nanosecond),
                };
                Field::new(&col.name, data_type, true)
            }).collect();
            
            let schema = Schema::new(fields);
            
            // Build arrays
            let arrays: Result<Vec<_>, _> = (0..self.columns.len()).map(|col_idx| {
                let column = &self.columns[col_idx];
                match column.column_type {
                    SasColumnType::String => {
                        let mut builder = StringBuilder::new();
                        for row in &rows {
                            match &row[col_idx] {
                                SasValue::String(s) => builder.append_value(s),
                                SasValue::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        }
                        Ok(Arc::new(builder.finish()) as ArrayRef)
                    }
                    SasColumnType::Numeric => {
                        let mut builder = Float64Builder::new();
                        for row in &rows {
                            match row[col_idx] {
                                SasValue::Numeric(n) => builder.append_value(n),
                                SasValue::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        }
                        Ok(Arc::new(builder.finish()) as ArrayRef)
                    }
                    // Add other types as needed
                    _ => {
                        let mut builder = Float64Builder::new();
                        for _ in &rows {
                            builder.append_null();
                        }
                        Ok(Arc::new(builder.finish()) as ArrayRef)
                    }
                }
            }).collect();
            
            let arrays = arrays?;
            let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;
            Ok(batch)
        }
    }
}

#[cfg(feature = "polars")]
pub mod polars_integration {
    use super::*;
    use polars::prelude::*;
    
    impl ChunkIterator {
        pub fn to_polars_dataframe(&mut self) -> PolarsResult<DataFrame> {
            #[cfg(feature = "arrow")]
            {
                let batch = self.to_arrow_batch().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                DataFrame::try_from(batch)
            }
            #[cfg(not(feature = "arrow"))]
            {
                Err(PolarsError::ComputeError("Arrow feature required for Polars integration".into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_functionality() {
        // This test would require a sample SAS file
        // let mut reader = SasChunkedReader::new("test.sas7bdat", 1000).unwrap();
        // 
        // while let Some(chunk_iter) = reader.chunk_iterator().unwrap() {
        //     for row_result in chunk_iter {
        //         let row = row_result.unwrap();
        //         println!("Row: {:?}", row);
        //     }
        // }
    }
}