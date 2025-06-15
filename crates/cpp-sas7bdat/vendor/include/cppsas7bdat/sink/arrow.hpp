/**
 *  \file include/cppsas7bdat/sink/arrow.hpp
 *
 *  \brief Apache Arrow datasink
 *
 *  \author Generated based on cppsas7bdat CSV sink
 */
#ifndef _CPP_SAS7BDAT_SINK_ARROW_HPP_
#define _CPP_SAS7BDAT_SINK_ARROW_HPP_


#include <cppsas7bdat/column.hpp>
#include <arrow/api.h>
#include <arrow/record_batch.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <arrow/status.h>
#include <arrow/c/bridge.h>  // For C Data Interface
#include <arrow/io/api.h>    // For FileOutputStream  
#include <arrow/ipc/api.h>   // For IPC writer
#include <memory>
#include <vector>
#include <string>

namespace cppsas7bdat {
namespace datasink {
namespace detail {

class arrow_sink {
private:
    COLUMNS columns;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    int64_t chunk_size_;
    int64_t current_row_count_;
    
    // Convert SAS column type to Arrow DataType
    std::shared_ptr<arrow::DataType> sas_to_arrow_type(cppsas7bdat::Column::Type type) {
        switch (type) {
            case cppsas7bdat::Column::Type::string:
                return arrow::utf8();
            case cppsas7bdat::Column::Type::integer:
                return arrow::int64();
            case cppsas7bdat::Column::Type::number:
                return arrow::float64();
            case cppsas7bdat::Column::Type::datetime:
                return arrow::timestamp(arrow::TimeUnit::MICRO); // microsecond precision
            case cppsas7bdat::Column::Type::date:
                return arrow::date32(); // days since epoch
            case cppsas7bdat::Column::Type::time:
                return arrow::time64(arrow::TimeUnit::MICRO); // microseconds since midnight
            case cppsas7bdat::Column::Type::unknown:
            default:
                return arrow::utf8(); // fallback to string for unknown types
        }
    }
    
    // Create appropriate array builder for the column type
    std::shared_ptr<arrow::ArrayBuilder> create_builder(cppsas7bdat::Column::Type type) {
        auto pool = arrow::default_memory_pool();
        switch (type) {
            case cppsas7bdat::Column::Type::string:
                return std::make_shared<arrow::StringBuilder>(pool);
            case cppsas7bdat::Column::Type::integer:
                return std::make_shared<arrow::Int64Builder>(pool);
            case cppsas7bdat::Column::Type::number:
                return std::make_shared<arrow::DoubleBuilder>(pool);
            case cppsas7bdat::Column::Type::datetime:
                return std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::MICRO), pool);
            case cppsas7bdat::Column::Type::date:
                return std::make_shared<arrow::Date32Builder>(pool);
            case cppsas7bdat::Column::Type::time:
                return std::make_shared<arrow::Time64Builder>(arrow::time64(arrow::TimeUnit::MICRO), pool);
            case cppsas7bdat::Column::Type::unknown:
            default:
                return std::make_shared<arrow::StringBuilder>(pool); // fallback to string for unknown types
        }
    }
    
    // Append value to the appropriate builder
    arrow::Status append_value(size_t col_idx, Column::PBUF p) {
        const auto& column = columns[col_idx];
        auto& builder = builders_[col_idx];
        
        switch (column.type) {
            case cppsas7bdat::Column::Type::string: {
                auto string_builder = static_cast<arrow::StringBuilder*>(builder.get());
                auto value = column.get_string(p);
                // String might be empty but probably never null in this implementation
                return string_builder->Append(std::string(value.data(), value.size()));
            }
            case cppsas7bdat::Column::Type::integer: {
                auto int_builder = static_cast<arrow::Int64Builder*>(builder.get());
                // Column interface returns INTEGER directly, not INTEGER*
                auto value = column.get_integer(p);
                return int_builder->Append(static_cast<int64_t>(value));
            }
            case cppsas7bdat::Column::Type::number: {
                auto double_builder = static_cast<arrow::DoubleBuilder*>(builder.get());
                auto value = column.get_number(p);
                if (std::isnan(value)) {
                    return double_builder->AppendNull();
                } else {
                    return double_builder->Append(value);
                }
            }
            case cppsas7bdat::Column::Type::datetime: {
                auto ts_builder = static_cast<arrow::TimestampBuilder*>(builder.get());
                auto value = column.get_datetime(p);
                // Check if datetime is "not-a-date-time" (Boost's way of representing null)
                if (value.is_not_a_date_time()) {
                    return ts_builder->AppendNull();
                } else {
                    // Convert boost::posix_time::ptime to microseconds since epoch
                    auto epoch = boost::posix_time::ptime(boost::gregorian::date(1970, 1, 1));
                    auto duration = value - epoch;
                    int64_t microseconds = duration.total_microseconds();
                    return ts_builder->Append(microseconds);
                }
            }
            case cppsas7bdat::Column::Type::date: {
                auto date_builder = static_cast<arrow::Date32Builder*>(builder.get());
                auto value = column.get_date(p);
                // Check if date is "not-a-date" (Boost's way of representing null)
                if (value.is_not_a_date()) {
                    return date_builder->AppendNull();
                } else {
                    // Convert boost::gregorian::date to days since epoch (1970-01-01)
                    boost::gregorian::date epoch(1970, 1, 1);
                    auto days = (value - epoch).days();
                    return date_builder->Append(static_cast<int32_t>(days));
                }
            }
            case cppsas7bdat::Column::Type::time: {
                auto time_builder = static_cast<arrow::Time64Builder*>(builder.get());
                auto value = column.get_time(p);
                // Check if time is "not-a-date-time" (Boost's way of representing null)
                if (value.is_not_a_date_time()) {
                    return time_builder->AppendNull();
                } else {
                    // Convert boost::posix_time::time_duration to microseconds since midnight
                    int64_t microseconds = value.total_microseconds();
                    return time_builder->Append(microseconds);
                }
            }
            case cppsas7bdat::Column::Type::unknown:
            default: {
                // Handle unknown column types by treating as string
                auto string_builder = static_cast<arrow::StringBuilder*>(builder.get());
                auto value = column.to_string(p);  // Use the generic to_string method
                return string_builder->Append(value);
            }
        }
        return arrow::Status::OK();
    }
    
    // Finalize current chunk and create a record batch
    arrow::Status finalize_chunk() {
        if (current_row_count_ == 0) {
            return arrow::Status::OK();
        }
        
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(builders_.size());
        
        for (auto& builder : builders_) {
            std::shared_ptr<arrow::Array> array;
            ARROW_RETURN_NOT_OK(builder->Finish(&array));
            arrays.push_back(array);
        }
        
        auto batch = arrow::RecordBatch::Make(schema_, current_row_count_, arrays);
        batches_.push_back(batch);
        
        // Reset builders for next chunk
        for (size_t i = 0; i < builders_.size(); ++i) {
            builders_[i] = create_builder(columns[i].type);
        }
        current_row_count_ = 0;
        
        return arrow::Status::OK();
    }
    

public:
    explicit arrow_sink(int64_t chunk_size = 65536) noexcept 
        : chunk_size_(chunk_size), current_row_count_(0) {}
    
    void set_properties(const Properties& _properties) {
        columns = COLUMNS(_properties.columns);
        
        // Create Arrow schema
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(columns.size());
        
        for (const auto& column : columns) {
            auto arrow_type = sas_to_arrow_type(column.type);
            fields.push_back(arrow::field(column.name, arrow_type));
        }
        
        schema_ = arrow::schema(fields);
        
        // Initialize builders
        builders_.reserve(columns.size());
        for (const auto& column : columns) {
            builders_.push_back(create_builder(column.type));
        }
    }
    
    void push_row([[maybe_unused]] size_t irow, Column::PBUF p) {
        for (size_t i = 0; i < columns.size(); ++i) {
            auto status = append_value(i, p);
            if (!status.ok()) {
                // Handle error - in production code you might want to throw or return error
                // For now, we'll just skip the problematic value
                continue;
            }
        }
        
        current_row_count_++;
        
        // Check if we should finalize current chunk
        if (current_row_count_ >= chunk_size_) {
            auto status = finalize_chunk();
            // Handle potential error from finalize_chunk if needed
        }
    }
    
    void end_of_data() const noexcept {
        // Finalize any remaining data in the last chunk
        if (current_row_count_ > 0) {
            auto status = const_cast<arrow_sink*>(this)->finalize_chunk();
            // Handle potential error from finalize_chunk if needed
        }
    }
    
    // Get the Arrow schema
    std::shared_ptr<arrow::Schema> get_schema() const {
        return schema_;
    }
    
    // Get all record batches
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& get_record_batches() const {
        return batches_;
    }
    
    // Get a single concatenated table (if you prefer table over record batches)
    arrow::Result<std::shared_ptr<arrow::Table>> get_table() const {
        if (batches_.empty()) {
            return arrow::Status::Invalid("No data available");
        }
        return arrow::Table::FromRecordBatches(schema_, batches_);
    }
    
    // Export to Arrow C Data Interface (for zero-copy interop with other systems)
    arrow::Status export_record_batch(size_t batch_index, ArrowArray* c_array, ArrowSchema* c_schema) const {
        if (batch_index >= batches_.size()) {
            return arrow::Status::IndexError("Batch index out of range");
        }
        
        // Export schema (only needed once)
        if (c_schema != nullptr) {
            ARROW_RETURN_NOT_OK(arrow::ExportSchema(*schema_, c_schema));
        }
        
        // Export record batch
        ARROW_RETURN_NOT_OK(arrow::ExportRecordBatch(*batches_[batch_index], c_array));
        
        return arrow::Status::OK();
    }
};

// File-based sink that writes to Arrow IPC format
class arrow_file_sink : public arrow_sink {
private:
    std::string filename_;
    
public:
    explicit arrow_file_sink(const char* filename, int64_t chunk_size = 65536)
        : arrow_sink(chunk_size), filename_(filename) {}
    
    void end_of_data() const noexcept {
        // Finalize any remaining data
        const_cast<arrow_file_sink*>(this)->arrow_sink::end_of_data();
        
        // Write to file in Arrow IPC format
        auto file_result = arrow::io::FileOutputStream::Open(filename_);
        if (!file_result.ok()) {
            // Handle file open error
            return;
        }
        auto file = file_result.ValueOrDie();
        
        auto writer_result = arrow::ipc::MakeFileWriter(file, get_schema());
        if (!writer_result.ok()) {
            // Handle writer creation error
            return;
        }
        auto writer = writer_result.ValueOrDie();
        
        // Write all record batches
        for (const auto& batch : get_record_batches()) {
            auto status = writer->WriteRecordBatch(*batch);
            if (!status.ok()) {
                // Handle write error
                continue;
            }
        }
        
        auto status = writer->Close();
        // Handle close error if needed
    }
};

} // namespace detail

struct arrow_factory {
    auto operator()(int64_t chunk_size = 65536) const noexcept {
        return detail::arrow_sink(chunk_size);
    }
    
    auto operator()(const char* filename, int64_t chunk_size = 65536) const {
        return detail::arrow_file_sink(filename, chunk_size);
    }
} arrow;

} // namespace datasink
} // namespace cppsas7bdat

#endif // _CPP_SAS7BDAT_SINK_ARROW_HPP_