// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <filesystem>
#include <arrow/util/future.h>
#include <lakesoul/lakesoul_data_reader.h>

namespace lakesoul {

LakeSoulDataReader::LakeSoulDataReader(std::shared_ptr<arrow::Schema> schema,
                                       const std::vector<std::string>& file_urls,
                                       const std::vector<std::pair<std::string, std::string>>& partition_info)
    : schema_(std::move(schema))
    , file_urls_(file_urls)
    , partition_info_(partition_info)
{
}

int LakeSoulDataReader::GetBatchSize() const
{
    return batch_size_;
}

void LakeSoulDataReader::SetBatchSize(int batch_size)
{
    batch_size_ = batch_size >= 1 ? batch_size : 1;
}

int LakeSoulDataReader::GetThreadNum() const
{
    return thread_num_;
}

void LakeSoulDataReader::SetThreadNum(int thread_num)
{
    thread_num_ = thread_num >= 1 ? thread_num : 1;
}

lakesoul::IOConfig* LakeSoulDataReader::CreateIOConfig()
{
    ArrowSchema c_schema;
    auto status = arrow::ExportSchema(*schema_, &c_schema);
    if (!status.ok())
    {
        std::ostringstream sout;
        sout << "Fail to export schema: " << status.ToString();
        std::string message = sout.str();
        std::cerr << message << std::endl;
        throw std::runtime_error(message);
    }
    lakesoul::IOConfigBuilder* builder = lakesoul::new_lakesoul_io_config_builder();
    for (const std::string& file_url : file_urls_)
        builder = lakesoul::lakesoul_config_builder_add_single_file(builder, file_url.c_str());
    builder = lakesoul::lakesoul_config_builder_set_batch_size(builder, batch_size_);
    builder = lakesoul::lakesoul_config_builder_set_thread_num(builder, thread_num_);
    builder = lakesoul::lakesoul_config_builder_set_schema(builder, reinterpret_cast<lakesoul::c_ptrdiff_t>(&c_schema));
    for (auto&& [key, value] : partition_info_)
        builder = lakesoul::lakesoul_config_builder_set_default_column_value(builder, key.c_str(), value.c_str());
    lakesoul::IOConfig* io_config = lakesoul::create_lakesoul_io_config_from_builder(builder);
    return io_config;
}

lakesoul::TokioRuntime* LakeSoulDataReader::CreateTokioRuntime()
{
    lakesoul::TokioRuntimeBuilder* builder = lakesoul::new_tokio_runtime_builder();
    builder = lakesoul::tokio_runtime_builder_set_thread_num(builder, thread_num_);
    lakesoul::TokioRuntime* tokio_runtime = lakesoul::create_tokio_runtime_from_builder(builder);
    return tokio_runtime;
}

std::shared_ptr<lakesoul::CResult<lakesoul::Reader>> LakeSoulDataReader::CreateReader()
{
    lakesoul::IOConfig* io_config = CreateIOConfig();
    lakesoul::TokioRuntime* tokio_runtime = CreateTokioRuntime();
    lakesoul::CResult<lakesoul::Reader>* result = lakesoul::create_lakesoul_reader_from_config(io_config, tokio_runtime);
    std::shared_ptr<lakesoul::CResult<lakesoul::Reader>> reader(result, [](lakesoul::CResult<lakesoul::Reader>* ptr)
    {
        lakesoul::free_lakesoul_reader(ptr);
        //std::cerr << "lakesoul::free_lakesoul_reader called\n";
    });
    const char* err = lakesoul::check_reader_created(result);
    if (err != nullptr)
    {
        std::ostringstream sout;
        sout << "Fail to create reader: " << err;
        std::string message = sout.str();
        std::cerr << message << std::endl;
        throw std::runtime_error(message);
    }
    return reader;
}

void LakeSoulDataReader::StartReader()
{
    struct Closure
    {
        std::shared_ptr<LakeSoulDataReader> reader;
        arrow::Future<bool> future;
    };
    reader_ = CreateReader();
    auto future = arrow::Future<bool>::Make();
    Closure closure;
    closure.reader = shared_from_this();
    closure.future = future;
    lakesoul::start_reader_with_data(reader_.get(),
        &closure, +[](bool status, const char* err, const void* data)
    {
        Closure* closure = static_cast<Closure*>(const_cast<void*>(data));
        if (!status)
        {
            std::ostringstream sout;
            sout << "Fail to start reader";
            if (err != nullptr)
                sout << ": " << err;
            std::string message = sout.str();
            closure->future.MarkFinished(arrow::Status::IOError(std::move(message)));
        }
        else
        {
            closure->future.MarkFinished(true);
        }
    });
    future.Wait();
}

bool LakeSoulDataReader::IsFinished() const
{
    return finished_;
}

arrow::Future<std::shared_ptr<arrow::RecordBatch>> LakeSoulDataReader::ReadRecordBatchAsync()
{
    struct Closure
    {
        std::shared_ptr<LakeSoulDataReader> reader;
        arrow::Future<std::shared_ptr<arrow::RecordBatch>> future;
        ArrowArray c_arrow_array;
        ArrowSchema c_arrow_schema;
    };
    auto future = arrow::Future<std::shared_ptr<arrow::RecordBatch>>::Make();
    auto closure = std::make_unique<Closure>();
    closure->reader = shared_from_this();
    closure->future = future;
    Closure* closure_ptr = closure.release();
    const void* data = static_cast<const void*>(closure_ptr);
    lakesoul::next_record_batch_with_data(reader_.get(),
        reinterpret_cast<lakesoul::c_ptrdiff_t>(&closure_ptr->c_arrow_schema),
        reinterpret_cast<lakesoul::c_ptrdiff_t>(&closure_ptr->c_arrow_array),
        data, +[](int32_t n, const char* err, const void* data)
    {
        Closure* closure_ptr = static_cast<Closure*>(const_cast<void*>(data));
        std::unique_ptr<Closure> closure(closure_ptr);
        if (n < 0)
        {
            std::ostringstream sout;
            sout << "Fail to read record batch";
            if (err != nullptr)
                sout << ": " << err;
            std::string message = sout.str();
            closure->future.MarkFinished(arrow::Status::IOError(std::move(message)));
        }
        else if (n == 0)
        {
            closure->future.MarkFinished(nullptr);
            closure->reader->finished_ = true;
        }
        else
        {
            auto bat = arrow::ImportRecordBatch(&closure->c_arrow_array, &closure->c_arrow_schema).ValueOrDie();
            closure->future.MarkFinished(std::move(bat));
        }
    });
    return future;
}

} // namespace lakesoul
