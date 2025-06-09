// src/cpp/chunked_reader.cpp
#include "chunked_reader.hpp"
#include <stdexcept>
#include <cstring>

namespace cppsas7bdat {

// ChunkData implementations
ChunkData::ChunkData(size_t chunk_size) 
    : start_row(0), end_row(0), is_complete(false) {
    rows.reserve(chunk_size);
    row_buffers.reserve(chunk_size);
}

ChunkData::ChunkData(ChunkData&& other) noexcept 
    : rows(std::move(other.rows))
    , row_buffers(std::move(other.row_buffers))
    , start_row(other.start_row)
    , end_row(other.end_row)
    , is_complete(other.is_complete) {
}

ChunkData& ChunkData::operator=(ChunkData&& other) noexcept {
    if (this != &other) {
        rows = std::move(other.rows);
        row_buffers = std::move(other.row_buffers);
        start_row = other.start_row;
        end_row = other.end_row;
        is_complete = other.is_complete;
    }
    return *this;
}

void ChunkData::clear() {
    rows.clear();
    row_buffers.clear();
    start_row = 0;
    end_row = 0;
    is_complete = false;
}

bool ChunkData::is_full(size_t target_size) const {
    return rows.size() >= target_size;
}

// ChunkSink implementation
ChunkSink::ChunkSink(size_t chunk_size) 
    : chunk_size_(chunk_size)
    , current_chunk_(chunk_size)
    , properties_(nullptr)
    , finished_(false) {
}

void ChunkSink::set_properties(const Properties& properties) {
    properties_ = &properties;
}

size_t ChunkSink::calculate_row_buffer_size() const {
    if (!properties_) return 0;
    
    size_t total_size = 0;
    for (const auto& col : properties_->columns) {
        total_size += col.length();
    }
    return total_size;
}

void ChunkSink::push_row(size_t row_index, Column::PBUF row_data) {
    if (current_chunk_.rows.empty()) {
        current_chunk_.start_row = row_index;
    }
    
    // Store the raw buffer data
    size_t buffer_size = calculate_row_buffer_size();
    std::vector<uint8_t> buffer_copy(buffer_size);
    
    if (row_data && buffer_size > 0) {
        std::memcpy(buffer_copy.data(), row_data, buffer_size);
    }
    current_chunk_.row_buffers.push_back(std::move(buffer_copy));
    
    // Create column objects for this row
    std::vector<cppsas7bdat::Column> row_columns;
    
    if (properties_) {
        for (size_t i = 0; i < properties_->columns.size(); ++i) {
            // Create a copy of the column definition
            row_columns.push_back(properties_->columns[i]);
        }
    }
    
    current_chunk_.rows.push_back(std::move(row_columns));
    current_chunk_.end_row = row_index;
    
    if (current_chunk_.is_full(chunk_size_)) {
        current_chunk_.is_complete = true;
        completed_chunks_.push(std::move(current_chunk_));
        current_chunk_ = ChunkData(chunk_size_);
    }
}

void ChunkSink::end_of_data() {
    // Push any remaining data as final chunk
    if (!current_chunk_.rows.empty()) {
        current_chunk_.is_complete = true;
        completed_chunks_.push(std::move(current_chunk_));
        current_chunk_ = ChunkData(chunk_size_);
    }
    finished_ = true;
}

bool ChunkSink::has_chunk() const {
    return !completed_chunks_.empty();
}

ChunkData ChunkSink::get_next_chunk() {
    if (completed_chunks_.empty()) {
        return ChunkData(); // Empty chunk
    }
    
    ChunkData chunk = std::move(completed_chunks_.front());
    completed_chunks_.pop();
    return chunk;
}

bool ChunkSink::is_finished() const {
    return finished_;
}

const Properties& ChunkSink::properties() const {
    if (!properties_) {
        throw std::runtime_error("Properties not set");
    }
    return *properties_;
}

// ChunkedReader implementation
ChunkedReader::ChunkedReader(const std::string& filename, size_t chunk_size)
    : chunk_sink_(std::make_unique<ChunkSink>(chunk_size))
    , chunk_size_(chunk_size)
    , initialized_(false) {
    
    try {
        // Create the ifstream datasource
        auto datasource = cppsas7bdat::datasource::ifstream(filename.c_str());

        // Create the reader with the datasource and sink
        reader_ = std::make_unique<cppsas7bdat::Reader>(
            std::move(datasource),
            *chunk_sink_
        );
        initialized_ = true;
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to create SAS reader: ") + e.what());
    }
}

bool ChunkedReader::read_next_chunk() {
    if (!initialized_) {
        throw std::runtime_error("Reader not properly initialized");
    }
    
    if (chunk_sink_->is_finished()) {
        return false; // No more data
    }
    
    try {
        // Read rows until we have a chunk or reach end of file
        size_t rows_to_read = chunk_size_;
        bool has_more_data = reader_->read_rows(rows_to_read);
        
        // Return true if we read some data or have a completed chunk
        return has_more_data || chunk_sink_->has_chunk();
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Error reading chunk: ") + e.what());
    }
}

ChunkData ChunkedReader::get_chunk() {
    return chunk_sink_->get_next_chunk();
}

bool ChunkedReader::has_chunk() const {
    return chunk_sink_->has_chunk();
}

const Properties& ChunkedReader::properties() const {
    if (!initialized_) {
        throw std::runtime_error("Reader not properly initialized");
    }
    return chunk_sink_->properties();
}

void ChunkedReader::ensure_initialized() {
    if (!initialized_) {
        throw std::runtime_error("Reader not properly initialized");
    }
}

} // namespace cppsas7bdat