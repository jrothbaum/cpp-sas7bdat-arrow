/**
 * @file src/arrow_ffi.h  
 * @brief C FFI interface for SAS7BDAT to Arrow conversion (Rust compatible)
 */

#ifndef CPPSAS7BDAT_ARROW_FFI_H
#define CPPSAS7BDAT_ARROW_FFI_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations for Arrow C Data Interface
struct ArrowSchema;
struct ArrowArray;

// Opaque handle for the SAS reader
typedef struct SasArrowReader SasArrowReader;

// Error codes
typedef enum {
    SAS_ARROW_OK = 0,
    SAS_ARROW_ERROR_FILE_NOT_FOUND = 1,
    SAS_ARROW_ERROR_INVALID_FILE = 2,
    SAS_ARROW_ERROR_OUT_OF_MEMORY = 3,
    SAS_ARROW_ERROR_ARROW_ERROR = 4,
    SAS_ARROW_ERROR_END_OF_DATA = 5,
    SAS_ARROW_ERROR_INVALID_BATCH_INDEX = 6,
    SAS_ARROW_ERROR_NULL_POINTER = 7
} SasArrowErrorCode;

// Reader info structure
typedef struct {
    uint64_t num_rows;
    uint32_t num_columns;
    uint32_t num_batches;
    uint32_t chunk_size;
} SasArrowReaderInfo;

// Column information
typedef struct {
    const char* name;
    const char* type_name;  // "string", "int64", "float64", "timestamp", "date32", "time64"
    uint32_t index;
} SasArrowColumnInfo;

/**
 * Create a new SAS Arrow reader from a file path
 * 
 * @param file_path Path to the .sas7bdat file
 * @param chunk_size Number of rows per batch (0 = default 65536)
 * @param reader Output pointer to the created reader
 * @return Error code
 */
SasArrowErrorCode sas_arrow_reader_new(
    const char* file_path,
    uint32_t chunk_size,
    SasArrowReader** reader
);

/**
 * Get basic information about the SAS file
 * 
 * @param reader The SAS reader instance
 * @param info Output structure with file information
 * @return Error code
 */
SasArrowErrorCode sas_arrow_reader_get_info(
    const SasArrowReader* reader,
    SasArrowReaderInfo* info
);

/**
 * Get column information
 * 
 * @param reader The SAS reader instance
 * @param column_index Zero-based column index
 * @param column_info Output structure with column information
 * @return Error code
 */
SasArrowErrorCode sas_arrow_reader_get_column_info(
    const SasArrowReader* reader,
    uint32_t column_index,
    SasArrowColumnInfo* column_info
);

/**
 * Get the Arrow schema (must be called before getting batches)
 * 
 * @param reader The SAS reader instance
 * @param schema Output Arrow schema (caller must release)
 * @return Error code
 */
SasArrowErrorCode sas_arrow_reader_get_schema(
    const SasArrowReader* reader,
    struct ArrowSchema* schema
);

/**
 * Read all data and get a specific record batch
 * 
 * @param reader The SAS reader instance
 * @param batch_index Zero-based batch index
 * @param array Output Arrow array (caller must release)
 * @return Error code
 */
SasArrowErrorCode sas_arrow_reader_get_batch(
    SasArrowReader* reader,
    uint32_t batch_index,
    struct ArrowArray* array
);

/**
 * Get record batch with schema in one call
 * 
 * @param reader The SAS reader instance
 * @param batch_index Zero-based batch index
 * @param array Output Arrow array (caller must release)
 * @param schema Output Arrow schema (caller must release)
 * @return Error code
 */
SasArrowErrorCode sas_arrow_reader_get_batch_with_schema(
    SasArrowReader* reader,
    uint32_t batch_index,
    struct ArrowArray* array,
    struct ArrowSchema* schema
);

/**
 * Stream interface: get next batch (reads file progressively)
 * 
 * @param reader The SAS reader instance
 * @param array Output Arrow array (caller must release)
 * @return Error code (SAS_ARROW_ERROR_END_OF_DATA when finished)
 */
SasArrowErrorCode sas_arrow_reader_next_batch(
    SasArrowReader* reader,
    struct ArrowArray* array
);

/**
 * Reset the stream reader to the beginning
 * 
 * @param reader The SAS reader instance
 * @return Error code
 */
SasArrowErrorCode sas_arrow_reader_reset(SasArrowReader* reader);

/**
 * Get the last error message (thread-local)
 * 
 * @return Pointer to error message string (valid until next call)
 */
const char* sas_arrow_get_last_error(void);

/**
 * Destroy the SAS reader and free resources
 * 
 * @param reader The SAS reader instance to destroy
 */
void sas_arrow_reader_destroy(SasArrowReader* reader);

// Utility functions for error handling

/**
 * Convert error code to human-readable string
 * 
 * @param error_code The error code
 * @return Static string describing the error
 */
const char* sas_arrow_error_message(SasArrowErrorCode error_code);

/**
 * Check if error code indicates success
 * 
 * @param error_code The error code to check
 * @return true if successful, false otherwise
 */
bool sas_arrow_is_ok(SasArrowErrorCode error_code);

#ifdef __cplusplus
}
#endif


#endif // CPPSAS7BDAT_ARROW_FFI_H