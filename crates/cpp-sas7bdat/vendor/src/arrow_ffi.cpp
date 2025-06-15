/**
 * @file src/arrow_ffi.cpp
 * @brief C FFI implementation for SAS7BDAT to Arrow conversion
 */

#include <cppsas7bdat/reader.hpp>
#include <cppsas7bdat/source/ifstream.hpp>
#include <cppsas7bdat/sink/arrow.hpp>
#include <arrow/c/bridge.h>
#include <memory>
#include <string>
#include <thread>

// Forward declarations for C interface
extern "C" {

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
    const char* type_name;
    uint32_t index;
} SasArrowColumnInfo;

} // extern "C"

// Thread-local error message storage
thread_local std::string g_last_error;

// Internal SAS reader structure
struct SasArrowReader {
    std::unique_ptr<cppsas7bdat::datasink::detail::arrow_sink> sink;
    std::unique_ptr<cppsas7bdat::Reader> reader;
    std::string file_path;
    uint32_t chunk_size;
    bool data_loaded;
    size_t current_batch_index;  // For streaming
    
    SasArrowReader(const std::string& path, uint32_t chunk_sz) 
        : file_path(path), chunk_size(chunk_sz), data_loaded(false), current_batch_index(0) {}
};

// Helper function to set error message
static void set_error(const std::string& message) {
    g_last_error = message;
}

// Helper function to convert C++ exceptions to error codes
template<typename Func>
static SasArrowErrorCode safe_call(Func&& func) {
    try {
        return func();
    } catch (const std::bad_alloc&) {
        set_error("Out of memory");
        return SAS_ARROW_ERROR_OUT_OF_MEMORY;
    } catch (const std::exception& e) {
        set_error(std::string("Error: ") + e.what());
        return SAS_ARROW_ERROR_ARROW_ERROR;
    } catch (...) {
        set_error("Unknown error occurred");
        return SAS_ARROW_ERROR_ARROW_ERROR;
    }
}

extern "C" {

SasArrowErrorCode sas_arrow_reader_new(
    const char* file_path,
    uint32_t chunk_size,
    SasArrowReader** reader
) {
    if (!file_path || !reader) {
        set_error("Null pointer provided");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        auto chunk_sz = chunk_size == 0 ? 65536U : chunk_size;
        auto sas_reader = std::make_unique<SasArrowReader>(file_path, chunk_sz);
        
        // Create the Arrow sink
        sas_reader->sink = std::make_unique<cppsas7bdat::datasink::detail::arrow_sink>(static_cast<int64_t>(chunk_sz));
        
        // Try to open the file to validate it exists and is readable
        try {
            auto data_source = cppsas7bdat::datasource::ifstream(file_path);
            sas_reader->reader = std::make_unique<cppsas7bdat::Reader>(
                std::move(data_source), *sas_reader->sink
            );
        } catch (const std::exception& e) {
            set_error(std::string("Failed to open SAS file: ") + e.what());
            return SAS_ARROW_ERROR_FILE_NOT_FOUND;
        }
        
        *reader = sas_reader.release();
        return SAS_ARROW_OK;
    });
}

SasArrowErrorCode sas_arrow_reader_get_info(
    const SasArrowReader* reader,
    SasArrowReaderInfo* info
) {
    if (!reader || !info) {
        set_error("Null pointer provided");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        // Ensure data is loaded
        if (!reader->data_loaded) {
            const_cast<SasArrowReader*>(reader)->reader->read_all();
            const_cast<SasArrowReader*>(reader)->data_loaded = true;
        }
        
        auto schema = reader->sink->get_schema();
        auto batches = reader->sink->get_record_batches();
        
        info->num_columns = static_cast<uint32_t>(schema->num_fields());
        info->num_batches = static_cast<uint32_t>(batches.size());
        info->chunk_size = reader->chunk_size;
        
        // Calculate total rows - fix sign conversion warning
        uint64_t total_rows = 0;
        for (const auto& batch : batches) {
            total_rows += static_cast<uint64_t>(batch->num_rows());
        }
        info->num_rows = total_rows;
        
        return SAS_ARROW_OK;
    });
}

SasArrowErrorCode sas_arrow_reader_get_column_info(
    const SasArrowReader* reader,
    uint32_t column_index,
    SasArrowColumnInfo* column_info
) {
    if (!reader || !column_info) {
        set_error("Null pointer provided");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        // Ensure data is loaded to get schema
        if (!reader->data_loaded) {
            const_cast<SasArrowReader*>(reader)->reader->read_all();
            const_cast<SasArrowReader*>(reader)->data_loaded = true;
        }
        
        auto schema = reader->sink->get_schema();
        if (column_index >= static_cast<uint32_t>(schema->num_fields())) {
            set_error("Column index out of range");
            return SAS_ARROW_ERROR_INVALID_BATCH_INDEX;
        }
        
        // Fix sign conversion warning
        auto field = schema->field(static_cast<int>(column_index));
        column_info->name = field->name().c_str();
        column_info->type_name = field->type()->ToString().c_str();
        column_info->index = column_index;
        
        return SAS_ARROW_OK;
    });
}

SasArrowErrorCode sas_arrow_reader_get_schema(
    const SasArrowReader* reader,
    struct ArrowSchema* schema
) {
    if (!reader || !schema) {
        set_error("Null pointer provided");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        // Ensure data is loaded
        if (!reader->data_loaded) {
            const_cast<SasArrowReader*>(reader)->reader->read_all();
            const_cast<SasArrowReader*>(reader)->data_loaded = true;
        }
        
        auto arrow_schema = reader->sink->get_schema();
        auto status = arrow::ExportSchema(*arrow_schema, schema);
        if (!status.ok()) {
            set_error("Failed to export Arrow schema: " + status.ToString());
            return SAS_ARROW_ERROR_ARROW_ERROR;
        }
        
        return SAS_ARROW_OK;
    });
}

SasArrowErrorCode sas_arrow_reader_get_batch(
    SasArrowReader* reader,
    uint32_t batch_index,
    struct ArrowArray* array
) {
    if (!reader || !array) {
        set_error("Null pointer provided");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        // Ensure data is loaded
        if (!reader->data_loaded) {
            reader->reader->read_all();
            reader->data_loaded = true;
        }
        
        auto status = reader->sink->export_record_batch(batch_index, array, nullptr);
        if (!status.ok()) {
            set_error("Failed to export record batch: " + status.ToString());
            return SAS_ARROW_ERROR_ARROW_ERROR;
        }
        
        return SAS_ARROW_OK;
    });
}

SasArrowErrorCode sas_arrow_reader_get_batch_with_schema(
    SasArrowReader* reader,
    uint32_t batch_index,
    struct ArrowArray* array,
    struct ArrowSchema* schema
) {
    if (!reader || !array || !schema) {
        set_error("Null pointer provided");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        // Ensure data is loaded
        if (!reader->data_loaded) {
            reader->reader->read_all();
            reader->data_loaded = true;
        }
        
        auto status = reader->sink->export_record_batch(batch_index, array, schema);
        if (!status.ok()) {
            set_error("Failed to export record batch with schema: " + status.ToString());
            return SAS_ARROW_ERROR_ARROW_ERROR;
        }
        
        return SAS_ARROW_OK;
    });
}

SasArrowErrorCode sas_arrow_reader_next_batch(
    SasArrowReader* reader,
    struct ArrowArray* array
) {
    if (!reader || !array) {
        set_error("Null pointer provided");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        if (!reader->data_loaded) {
            reader->reader->read_all();
            reader->data_loaded = true;
        }
        
        auto batches = reader->sink->get_record_batches();
        if (reader->current_batch_index >= batches.size()) {
            return SAS_ARROW_ERROR_END_OF_DATA;
        }
        
        auto status = reader->sink->export_record_batch(
            reader->current_batch_index, array, nullptr
        );
        if (!status.ok()) {
            set_error("Failed to export next batch: " + status.ToString());
            return SAS_ARROW_ERROR_ARROW_ERROR;
        }
        
        reader->current_batch_index++;
        return SAS_ARROW_OK;
    });
}

SasArrowErrorCode sas_arrow_reader_reset(SasArrowReader* reader) {
    if (!reader) {
        set_error("Null pointer provided");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    reader->current_batch_index = 0;
    return SAS_ARROW_OK;
}

const char* sas_arrow_get_last_error(void) {
    return g_last_error.c_str();
}

void sas_arrow_reader_destroy(SasArrowReader* reader) {
    delete reader;
}

const char* sas_arrow_error_message(SasArrowErrorCode error_code) {
    switch (error_code) {
        case SAS_ARROW_OK: return "Success";
        case SAS_ARROW_ERROR_FILE_NOT_FOUND: return "File not found or cannot be opened";
        case SAS_ARROW_ERROR_INVALID_FILE: return "Invalid SAS7BDAT file format";
        case SAS_ARROW_ERROR_OUT_OF_MEMORY: return "Out of memory";
        case SAS_ARROW_ERROR_ARROW_ERROR: return "Arrow library error";
        case SAS_ARROW_ERROR_END_OF_DATA: return "End of data reached";
        case SAS_ARROW_ERROR_INVALID_BATCH_INDEX: return "Invalid batch index";
        case SAS_ARROW_ERROR_NULL_POINTER: return "Null pointer provided";
        default: return "Unknown error";
    }
}

bool sas_arrow_is_ok(SasArrowErrorCode error_code) {
    return error_code == SAS_ARROW_OK;
}

} // extern "C"