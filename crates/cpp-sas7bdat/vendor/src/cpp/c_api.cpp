// src/cpp/c_api.cpp
#include "c_api.h"
#include "chunked_reader.hpp"
#include <memory>
#include <cstring>
#include <string>

// Internal state management
struct ChunkedReaderState {
    std::unique_ptr<cppsas7bdat::ChunkedReader> reader;
    cppsas7bdat::ChunkData current_chunk;
    bool has_current_chunk = false;
    
    ChunkedReaderState(const std::string& filename, size_t chunk_size)
        : reader(std::make_unique<cppsas7bdat::ChunkedReader>(filename, chunk_size)) {}
};

struct ChunkIteratorState {
    cppsas7bdat::ChunkData chunk;
    size_t current_row_index = 0;
    std::vector<CColumnValue> current_row_values; // Buffer for current row
    
    ChunkIteratorState(cppsas7bdat::ChunkData&& chunk_data)
        : chunk(std::move(chunk_data)) {
        current_row_values.resize(chunk.rows.empty() ? 0 : chunk.rows[0].size());
    }
};

extern "C" {

// Core reader functions
ChunkedReaderHandle chunked_reader_create(const char* filename, size_t chunk_size) {
    if (!filename) return nullptr;
    
    try {
        auto* state = new ChunkedReaderState(filename, chunk_size);
        return static_cast<ChunkedReaderHandle>(state);
    } catch (const std::exception& e) {
        // Log error if needed
        return nullptr;
    } catch (...) {
        return nullptr;
    }
}

int chunked_reader_get_properties(ChunkedReaderHandle handle, CProperties* properties) {
    if (!handle || !properties) return -1;
    
    try {
        auto* state = static_cast<ChunkedReaderState*>(handle);
        const auto& props = state->reader->properties();
        
        // Convert C++ properties to C structure
        properties->column_count = props.columns.size();
        properties->total_rows = 0; // cpp-sas7bdat doesn't provide total rows upfront
        
        if (properties->column_count == 0) {
            properties->columns = nullptr;
            return 0;
        }
        
        properties->columns = static_cast<CColumnInfo*>(
            malloc(properties->column_count * sizeof(CColumnInfo))
        );
        
        if (!properties->columns) return -1;
        
        for (size_t i = 0; i < properties->column_count; ++i) {
            const auto& col = props.columns[i];
            
            auto& c_col = properties->columns[i];
            // Allocate and copy name
            size_t name_len = col.name.length();
            char* name_copy = static_cast<char*>(malloc(name_len + 1));
            if (!name_copy) {
                // Cleanup previously allocated names
                for (size_t j = 0; j < i; ++j) {
                    free(const_cast<char*>(properties->columns[j].name));
                }
                free(properties->columns);
                return -1;
            }
            strcpy(name_copy, col.name.c_str());
            c_col.name = name_copy;
            
            // Map column type based on SAS format
            switch (col.type) {
                case cppsas7bdat::Column::Type::string:
                    c_col.column_type = 0; // string
                    break;
                case cppsas7bdat::Column::Type::number:
                    c_col.column_type = 1; // numeric  
                    break;
                case cppsas7bdat::Column::Type::date:   
                    c_col.column_type = 2; // date
                    break;
                case cppsas7bdat::Column::Type::datetime:
                    c_col.column_type = 3; // datetime
                    break;
                case cppsas7bdat::Column::Type::time: 
                    c_col.column_type = 4; // time
                    break;
                case cppsas7bdat::Column::Type::integer:
                    c_col.column_type = 1; // numeric (treat integers as numeric)
                    break;
                case cppsas7bdat::Column::Type::unknown:
                default:
                    c_col.column_type = 1; // default to numeric
                    break;
            }
            
            c_col.length = col.length();
        }
        
        return 0;
    } catch (...) {
        return -1;
    }
}

int chunked_reader_next_chunk(ChunkedReaderHandle handle, CChunkInfo* chunk_info) {
    if (!handle || !chunk_info) return -1;
    
    try {
        auto* state = static_cast<ChunkedReaderState*>(handle);
        
        // Try to read next chunk
        if (!state->reader->read_next_chunk()) {
            return 1; // No more data
        }
        
        if (!state->reader->has_chunk()) {
            return 1; // No chunk available
        }
        
        // Get the chunk and store it in state
        state->current_chunk = state->reader->get_chunk();
        state->has_current_chunk = true;
        
        // Fill chunk info
        chunk_info->row_count = state->current_chunk.rows.size();
        chunk_info->start_row = state->current_chunk.start_row;
        chunk_info->end_row = state->current_chunk.end_row;
        
        return 0; // Success
    } catch (...) {
        return -1;
    }
}

int chunked_reader_has_chunk(ChunkedReaderHandle handle) {
    if (!handle) return 0;
    
    try {
        auto* state = static_cast<ChunkedReaderState*>(handle);
        return state->has_current_chunk ? 1 : 0;
    } catch (...) {
        return 0;
    }
}

void chunked_reader_destroy(ChunkedReaderHandle handle) {
    if (handle) {
        delete static_cast<ChunkedReaderState*>(handle);
    }
}

// Chunk iterator functions
ChunkIteratorHandle chunk_iterator_create(ChunkedReaderHandle reader_handle) {
    if (!reader_handle) return nullptr;
    
    try {
        auto* reader_state = static_cast<ChunkedReaderState*>(reader_handle);
        
        if (!reader_state->has_current_chunk) {
            return nullptr;
        }
        
        // Move the chunk to iterator (reader no longer owns it)
        auto* iterator_state = new ChunkIteratorState(std::move(reader_state->current_chunk));
        reader_state->has_current_chunk = false;
        
        return static_cast<ChunkIteratorHandle>(iterator_state);
    } catch (...) {
        return nullptr;
    }
}

int chunk_iterator_next_row(ChunkIteratorHandle handle, CRowData* row_data) {
    if (!handle || !row_data) return -1;
    
    try {
        auto* state = static_cast<ChunkIteratorState*>(handle);
        
        if (state->current_row_index >= state->chunk.rows.size()) {
            return 1; // No more rows
        }
        
        const auto& row = state->chunk.rows[state->current_row_index];
        const void* row_buffer = state->chunk.get_row_buffer(state->current_row_index);
        
        row_data->column_count = row.size();
        row_data->values = static_cast<CColumnValue*>(
            malloc(row.size() * sizeof(CColumnValue))
        );
        
        if (!row_data->values && row.size() > 0) {
            return -1;
        }
        
        // Convert each column value
        for (size_t i = 0; i < row.size(); ++i) {
            const auto& col = row[i];
            auto& c_val = row_data->values[i];
            
            // Use the column's type to determine how to handle the data
            switch (col.type) {
                case cppsas7bdat::Column::Type::string: {
                    c_val.value_type = 1;
                    
                    if (row_buffer) {
                        auto str_val = col.get_string(row_buffer);
                        char* str_copy = static_cast<char*>(malloc(str_val.length() + 1));
                        if (str_copy) {
                            std::memcpy(str_copy, str_val.data(), str_val.length());
                            str_copy[str_val.length()] = '\0'; // Null terminate
                            c_val.string_val = str_copy;
                            c_val.is_null = 0;
                        } else {
                            c_val.string_val = nullptr;
                            c_val.is_null = 1;
                        }
                    } else {
                        c_val.string_val = nullptr;
                        c_val.is_null = 1;
                    }
                    c_val.numeric_val = 0.0;
                    break;
                }
                case cppsas7bdat::Column::Type::number:
                case cppsas7bdat::Column::Type::integer: {
                    c_val.value_type = 2;
                    c_val.string_val = nullptr;
                    
                    if (row_buffer) {
                        c_val.numeric_val = col.get_number(row_buffer);
                        c_val.is_null = 0; // TODO: Implement null detection
                    } else {
                        c_val.numeric_val = 0.0;
                        c_val.is_null = 1;
                    }
                    break;
                }
                case cppsas7bdat::Column::Type::date:
                case cppsas7bdat::Column::Type::datetime:
                case cppsas7bdat::Column::Type::time: {
                    c_val.value_type = 2; // Treat dates as numeric for now
                    c_val.string_val = nullptr;
                    
                    if (row_buffer) {
                        c_val.numeric_val = col.get_number(row_buffer);
                        c_val.is_null = 0;
                    } else {
                        c_val.numeric_val = 0.0;
                        c_val.is_null = 1;
                    }
                    break;
                }
                default: {
                    c_val.value_type = 0;
                    c_val.string_val = nullptr;
                    c_val.numeric_val = 0.0;
                    c_val.is_null = 1;
                    break;
                }
            }
        }
        
        state->current_row_index++;
        return 0;
    } catch (...) {
        return -1;
    }
}

int chunk_iterator_has_next(ChunkIteratorHandle handle) {
    if (!handle) return 0;
    
    try {
        auto* state = static_cast<ChunkIteratorState*>(handle);
        return (state->current_row_index < state->chunk.rows.size()) ? 1 : 0;
    } catch (...) {
        return 0;
    }
}

void chunk_iterator_destroy(ChunkIteratorHandle handle) {
    if (handle) {
        delete static_cast<ChunkIteratorState*>(handle);
    }
}

// Memory management functions
void free_row_data(CRowData* row_data) {
    if (!row_data || !row_data->values) return;
    
    // Free each string value that was allocated
    for (size_t i = 0; i < row_data->column_count; ++i) {
        if (row_data->values[i].string_val) {
            free(const_cast<char*>(row_data->values[i].string_val));
        }
    }
    
    // Free the values array
    free(row_data->values);
    row_data->values = nullptr;
    row_data->column_count = 0;
}

void free_properties(CProperties* properties) {
    if (!properties || !properties->columns) return;
    
    // Free each column name
    for (size_t i = 0; i < properties->column_count; ++i) {
        if (properties->columns[i].name) {
            free(const_cast<char*>(properties->columns[i].name));
        }
    }
    
    // Free the columns array
    free(properties->columns);
    properties->columns = nullptr;
    properties->column_count = 0;
    properties->total_rows = 0;
}

} // extern "C"