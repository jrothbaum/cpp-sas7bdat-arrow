use std::ffi::{CStr, CString};
use std::os::raw::{c_char};
use std::ptr;
use polars::prelude::*;
use polars_arrow;

// Error codes from your C FFI
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SasArrowErrorCode {
    SasArrowOk = 0,
    SasArrowErrorFileNotFound = 1,
    SasArrowErrorInvalidFile = 2,
    SasArrowErrorOutOfMemory = 3,
    SasArrowErrorArrowError = 4,
    SasArrowErrorEndOfData = 5,
    SasArrowErrorInvalidBatchIndex = 6,
    SasArrowErrorNullPointer = 7,
}

// Reader info structure
#[repr(C)]
#[derive(Debug, Clone, Copy)] 
pub struct SasArrowReaderInfo {
    pub num_rows: u64,
    pub num_columns: u32,
    pub num_batches: u32,
    pub chunk_size: u32,
}

// Arrow FFI structures (compatible with Arrow C Data Interface)
#[repr(C)]
pub struct CArrowSchema {
    format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    flags: i64,
    n_children: i64,
    children: *mut *mut CArrowSchema,
    dictionary: *mut CArrowSchema,
    release: Option<unsafe extern "C" fn(*mut CArrowSchema)>,
    private_data: *mut std::ffi::c_void,
}

#[repr(C)]
pub struct CArrowArray {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *mut *const std::ffi::c_void,
    children: *mut *mut CArrowArray,
    dictionary: *mut CArrowArray,
    release: Option<unsafe extern "C" fn(*mut CArrowArray)>,
    private_data: *mut std::ffi::c_void,
}

impl CArrowSchema {
    pub fn empty() -> Self {
        CArrowSchema {
            format: ptr::null(),
            name: ptr::null(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

impl CArrowArray {
    pub fn empty() -> Self {
        CArrowArray {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: ptr::null_mut(),
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

// Opaque handle for the SAS reader
#[repr(C)]
pub struct SasArrowReader {
    _private: [u8; 0],
}

// External C functions from your FFI
extern "C" {
    fn sas_arrow_reader_new(
        file_path: *const c_char,
        chunk_size: u32,
        reader: *mut *mut SasArrowReader,
    ) -> SasArrowErrorCode;

    fn sas_arrow_reader_get_info(
        reader: *const SasArrowReader,
        info: *mut SasArrowReaderInfo,
    ) -> SasArrowErrorCode;

    fn sas_arrow_reader_get_schema(
        reader: *const SasArrowReader,
        schema: *mut CArrowSchema,
    ) -> SasArrowErrorCode;

    fn sas_arrow_reader_get_batch_with_schema(
        reader: *mut SasArrowReader,
        batch_index: u32,
        array: *mut CArrowArray,
        schema: *mut CArrowSchema,
    ) -> SasArrowErrorCode;

    fn sas_arrow_reader_next_batch(
        reader: *mut SasArrowReader,
        array: *mut CArrowArray,
    ) -> SasArrowErrorCode;

    fn sas_arrow_reader_reset(reader: *mut SasArrowReader) -> SasArrowErrorCode;

    fn sas_arrow_reader_destroy(reader: *mut SasArrowReader);

    fn sas_arrow_get_last_error() -> *const c_char;

    fn sas_arrow_error_message(error_code: SasArrowErrorCode) -> *const c_char;
}

pub struct SasReader {
    reader: *mut SasArrowReader,
    info: SasArrowReaderInfo,
    cached_schema: Option<Schema>,
    cached_arrow_field: Option<polars_arrow::datatypes::Field>,
}

impl SasReader {
    /// Create a new SAS reader
    pub fn new(file_path: &str, chunk_size: Option<u32>) -> PolarsResult<Self> {
        let c_path = CString::new(file_path)
            .map_err(|e| PolarsError::ComputeError(format!("Invalid file path: {}", e).into()))?;
        
        let mut reader: *mut SasArrowReader = ptr::null_mut();
        let chunk_size = chunk_size.unwrap_or(0); // 0 = default
        
        let result = unsafe {
            sas_arrow_reader_new(c_path.as_ptr(), chunk_size, &mut reader)
        };
        
        if result != SasArrowErrorCode::SasArrowOk {
            return Err(Self::error_from_code(result));
        }
        
        // Get file info
        let mut info = SasArrowReaderInfo {
            num_rows: 0,
            num_columns: 0,
            num_batches: 0,
            chunk_size: 0,
        };
        
        let result = unsafe {
            sas_arrow_reader_get_info(reader, &mut info)
        };
        
        if result != SasArrowErrorCode::SasArrowOk {
            unsafe { sas_arrow_reader_destroy(reader) };
            return Err(Self::error_from_code(result));
        }
        
        Ok(SasReader { 
            reader, 
            info,
            cached_schema: None,
            cached_arrow_field: None,
        })
    }
    
    /// Get schema information (returns field names and types)
    pub fn get_schema_info(&mut self) -> PolarsResult<&Schema> {
        if self.cached_schema.is_none() {
            let mut c_schema = CArrowSchema::empty();
            
            let result = unsafe {
                sas_arrow_reader_get_schema(self.reader, &mut c_schema)
            };
            
            if result != SasArrowErrorCode::SasArrowOk {
                return Err(Self::error_from_code(result));
            }
            
            // Convert and cache both schemas
            let (polars_schema, arrow_field) = unsafe { 
                self.arrow_schema_to_polars_schema(&c_schema)? 
            };
            
            
            
            self.cached_schema = Some(polars_schema);
            self.cached_arrow_field = Some(arrow_field);
        }
        
        Ok(self.cached_schema.as_ref().unwrap())
    }
    
    /// Read all data as a single DataFrame
    // pub fn read_all(&mut self) -> PolarsResult<DataFrame> {
    //     let mut dataframes = Vec::new();
        
    //     // Reset to beginning
    //     let result = unsafe { sas_arrow_reader_reset(self.reader) };
    //     if result != SasArrowErrorCode::SasArrowOk {
    //         return Err(Self::error_from_code(result));
    //     }
        
    //     // Read all batches
    //     loop {
    //         match self.read_next_batch() {
    //             Ok(df) => dataframes.push(df),
    //             Err(e) => {
    //                 // Check if it's end of data
    //                 if e.to_string().contains("end of data") {
    //                     break;
    //                 }
    //                 return Err(e);
    //             }
    //         }
    //     }
        
    //     if dataframes.is_empty() {
    //         return Err(PolarsError::ComputeError("No data found in SAS file".into()));
    //     }
        
    //     // Concatenate all dataframes
    //     let mut result_df = dataframes.into_iter().next().unwrap();
    //     for df in dataframes.into_iter().skip(1) {
    //         result_df = result_df.vstack(&df)?;
    //     }
        
    //     Ok(result_df)
    // }
    
    /// Read the next batch as a DataFrame (streaming interface)
    pub fn read_next_batch(&mut self) -> PolarsResult<DataFrame> {
        // Ensure schema is cached
        self.get_schema_info()?;
        
        let mut c_array = CArrowArray::empty();
        
        let result = unsafe {
            sas_arrow_reader_next_batch(self.reader, &mut c_array)
        };
        
        if result == SasArrowErrorCode::SasArrowErrorEndOfData {
            return Err(PolarsError::ComputeError("End of data reached".into()));
        }
        
        if result != SasArrowErrorCode::SasArrowOk {
            return Err(Self::error_from_code(result));
        }
        
        // Use cached schema for conversion
        let arrow_field = self.cached_arrow_field.as_ref().unwrap().clone();
        let df = self.arrow_to_dataframe_with_field(c_array, arrow_field)?;
        
        Ok(df)
    }
    
    /// Read a specific batch by index
    pub fn read_batch(&mut self, batch_index: u32) -> PolarsResult<DataFrame> {
        if batch_index >= self.info.num_batches {
            return Err(PolarsError::ComputeError(
                format!("Batch index {} out of range (max: {})", batch_index, self.info.num_batches - 1).into()
            ));
        }
        
        // Ensure schema is cached
        self.get_schema_info()?;
        
        let mut c_array = CArrowArray::empty();
        let mut c_schema = CArrowSchema::empty();
        
        let result = unsafe {
            sas_arrow_reader_get_batch_with_schema(
                self.reader,
                batch_index,
                &mut c_array,
                &mut c_schema,
            )
        };
        
        if result != SasArrowErrorCode::SasArrowOk {
            return Err(Self::error_from_code(result));
        }
        
        // Use cached schema for conversion
        let arrow_field = self.cached_arrow_field.as_ref().unwrap().clone();
        self.arrow_to_dataframe_with_field(c_array, arrow_field)
    }
    
    /// Convert Arrow C Data Interface to Polars DataFrame using cached field
    fn arrow_to_dataframe_with_field(&self, c_array: CArrowArray, field: polars_arrow::datatypes::Field) -> PolarsResult<DataFrame> {
        unsafe {
            // Read the array data directly (taking ownership)
            let array_ptr = &c_array as *const CArrowArray as *const polars_arrow::ffi::ArrowArray;
            let arrow_array = std::ptr::read(array_ptr);
            
            // Get struct fields first before moving dtype
            let struct_fields = match &field.dtype {
                polars_arrow::datatypes::ArrowDataType::Struct(fields) => fields,
                _ => return Err(PolarsError::ComputeError("Expected struct data type from SAS data".into())),
            };
            
            // Import array from C using polars_arrow FFI with cached field
            let imported_array = polars_arrow::ffi::import_array_from_c(arrow_array, field.dtype.clone())
                .map_err(|e| PolarsError::ComputeError(format!("Failed to import array: {}", e).into()))?;
            
            // SAS data is always a struct (record batch) with multiple columns
            let struct_array = imported_array.as_any().downcast_ref::<polars_arrow::array::StructArray>()
                .ok_or_else(|| PolarsError::ComputeError("Expected struct array from SAS data".into()))?;
            
            let mut columns = Vec::new();
            for (i, struct_field) in struct_fields.iter().enumerate() {
                let col_array = struct_array.values()[i].clone();
                // Convert Arrow array to Polars Series
                let series = Series::from_arrow(struct_field.name.as_str().into(), col_array)
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to create column series: {}", e).into()))?;
                columns.push(series);
            }
            
            Ok(DataFrame::from_iter(columns))
        }
    }
    

    /// Convert Arrow C Data Interface to Polars DataFrame using FFI
    fn arrow_to_dataframe(&self, c_array: CArrowArray, c_schema: CArrowSchema) -> PolarsResult<DataFrame> {
        unsafe {
            // Read the schema data directly (taking ownership)
            let schema_ptr = &c_schema as *const CArrowSchema as *const polars_arrow::ffi::ArrowSchema;
            let arrow_schema = std::ptr::read(schema_ptr);
            
            // Import field from C schema using polars_arrow FFI
            let field = polars_arrow::ffi::import_field_from_c(&arrow_schema)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to import schema: {}", e).into()))?;
            
            self.arrow_to_dataframe_with_field(c_array, field)
        }
    }
    
    /// Convert Arrow schema to Polars schema
    unsafe fn arrow_schema_to_polars_schema(&self, c_schema: &CArrowSchema) -> PolarsResult<(Schema, polars_arrow::datatypes::Field)> {
        // Read the schema data directly (taking ownership)
        let schema_ptr = c_schema as *const CArrowSchema as *const polars_arrow::ffi::ArrowSchema;
        let arrow_schema = std::ptr::read_unaligned(schema_ptr);
        
        let field = polars_arrow::ffi::import_field_from_c(&arrow_schema)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to import schema: {}", e).into()))?;
        
        // SAS data is always a struct with multiple columns
        let struct_fields = match &field.dtype {
            polars_arrow::datatypes::ArrowDataType::Struct(fields) => fields,
            _ => return Err(PolarsError::ComputeError("Expected struct data type from SAS data".into())),
        };
        
        let mut schema_map = std::collections::BTreeMap::new();
        for struct_field in struct_fields {
            let polars_dtype = self.arrow_dtype_to_polars(&struct_field.dtype)?;
            schema_map.insert(struct_field.name.clone(), polars_dtype);
        }
        
        let polars_schema = Schema::from_iter(schema_map);
        Ok((polars_schema, field))
    }
    
    /// Convert Arrow data type to Polars data type
    fn arrow_dtype_to_polars(&self, arrow_type: &polars_arrow::datatypes::ArrowDataType) -> PolarsResult<DataType> {
        use polars_arrow::datatypes::ArrowDataType;
        
        let polars_type = match arrow_type {
            // SAS string columns -> UTF8
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::String,
            
            // SAS integer columns -> Int64
            ArrowDataType::Int64 => DataType::Int64,
            
            // SAS number columns -> Float64
            ArrowDataType::Float64 => DataType::Float64,
            
            // SAS datetime columns -> Timestamp with microsecond precision
            ArrowDataType::Timestamp(unit, _) => {
                let time_unit = match unit {
                    polars_arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microseconds,
                    polars_arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanoseconds,
                    polars_arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Milliseconds,
                    polars_arrow::datatypes::TimeUnit::Second => TimeUnit::Milliseconds,
                };
                DataType::Datetime(time_unit,None)
            },
            
            // SAS date columns -> Date32 (days since epoch)
            ArrowDataType::Date32 => DataType::Date,
            
            // SAS time columns -> Time64 with microsecond precision
            ArrowDataType::Time64(_) => DataType::Time,
            
            // Fallback for any unexpected types
            _ => {
                return Err(PolarsError::ComputeError(
                    format!("Unsupported SAS Arrow data type: {:?}", arrow_type).into()
                ));
            }
        };
        
        Ok(polars_type)
    }
    
    /// Reset the streaming reader to the beginning
    pub fn reset(&mut self) -> PolarsResult<()> {
        let result = unsafe { sas_arrow_reader_reset(self.reader) };
        
        if result != SasArrowErrorCode::SasArrowOk {
            return Err(Self::error_from_code(result));
        }
        
        Ok(())
    }
    
    /// Get reader information
    pub fn info(&self) -> &SasArrowReaderInfo {
        &self.info
    }
    
    /// Convert error code to PolarsError
    fn error_from_code(code: SasArrowErrorCode) -> PolarsError {
        let message = unsafe {
            let msg_ptr = sas_arrow_error_message(code);
            if msg_ptr.is_null() {
                "Unknown error".to_string()
            } else {
                CStr::from_ptr(msg_ptr).to_string_lossy().to_string()
            }
        };
        
        // Also get the last error message if available
        let last_error = unsafe {
            let error_ptr = sas_arrow_get_last_error();
            if !error_ptr.is_null() {
                let last_msg = CStr::from_ptr(error_ptr).to_string_lossy();
                if !last_msg.is_empty() {
                    format!("{}: {}", message, last_msg)
                } else {
                    message
                }
            } else {
                message
            }
        };
        
        match code {
            SasArrowErrorCode::SasArrowErrorFileNotFound => {
                PolarsError::IO {
                    error: std::io::Error::new(std::io::ErrorKind::NotFound, last_error).into(),
                    msg: None,
                }
            }
            SasArrowErrorCode::SasArrowErrorOutOfMemory => {
                PolarsError::ComputeError(format!("Out of memory: {}", last_error).into())
            }
            _ => PolarsError::ComputeError(last_error.into()),
        }
    }
}

impl Drop for SasReader {
    fn drop(&mut self) {
        if !self.reader.is_null() {
            unsafe {
                sas_arrow_reader_destroy(self.reader);
            }
        }
    }
}

// Convenience functions
impl SasReader {
    /// Create a reader and read all data in one call
    // pub fn read_sas(file_path: &str, chunk_size: Option<u32>) -> PolarsResult<DataFrame> {
    //     let mut reader = Self::new(file_path, chunk_size)?;
    //     reader.read_all()
    // }
    
    /// Create a reader and get just the schema
    pub fn read_sas_schema(file_path: &str) -> PolarsResult<Schema> {
        let mut reader = Self::new(file_path, None)?;
        Ok(reader.get_schema_info()?.clone())
    }
}

// Iterator implementation for streaming
pub struct SasBatchIterator {
    reader: SasReader,
    finished: bool,
}

impl SasBatchIterator {
    pub fn new(file_path: &str, chunk_size: Option<u32>) -> PolarsResult<Self> {
        let reader = SasReader::new(file_path, chunk_size)?;
        Ok(SasBatchIterator {
            reader,
            finished: false,
        })
    }

    /// Get the schema without reading any data
    pub fn schema(&mut self) -> PolarsResult<&Schema> {
        self.reader.get_schema_info()
    }

    /// Get reader info (number of rows, columns, batches, etc.)
    pub fn info(&self) -> &SasArrowReaderInfo {
        self.reader.info()
    }
}

impl Iterator for SasBatchIterator {
    type Item = PolarsResult<DataFrame>;
    
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        
        match self.reader.read_next_batch() {
            Ok(df) => Some(Ok(df)),
            Err(e) => {
                if e.to_string().contains("End of data") {
                    self.finished = true;
                    None
                } else {
                    self.finished = true;
                    Some(Err(e))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_read_sas_all() {
        // This test requires a real SAS file
        // let result = SasReader::read_sas("test.sas7bdat", None);
        // match result {
        //     Ok(df) => {
        //         println!("Read {} rows, {} columns", df.height(), df.width());
        //         println!("Schema: {:?}", df.schema());
        //     }
        //     Err(e) => println!("Error reading SAS file: {}", e),
        // }
    }
    
    #[test]
    fn test_streaming_read() {
        let iterator = SasBatchIterator::new("test.sas7bdat", Some(1000));
        match iterator {
            Ok(iter) => {
                for (i, batch_result) in iter.enumerate() {
                    match batch_result {
                        Ok(batch) => {
                            println!("Batch {}: {} rows", i, batch.height());
                        }
                        Err(e) => {
                            println!("Error in batch {}: {}", i, e);
                            break;
                        }
                    }
                }
            }
            Err(e) => println!("Error creating iterator: {}", e),
        }
    }
    
    #[test]
    fn test_schema_only() {
        let result = SasReader::read_sas_schema("test.sas7bdat");
        match result {
            Ok(schema) => {
                println!("Schema: {:?}", schema);
                for (name, dtype) in schema.iter() {
                    println!("Column: {} -> {:?}", name, dtype);
                }
            }
            Err(e) => println!("Error reading schema: {}", e),
        }
    }
}