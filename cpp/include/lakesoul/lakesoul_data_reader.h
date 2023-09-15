// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#ifndef LAKESOUL_DATA_READER_H
#define LAKESOUL_DATA_READER_H

#include <arrow/type.h>
#include <arrow/c/bridge.h>
#include <lakesoul_c_bindings.h>

namespace lakesoul {

class LakeSoulDataReader : public std::enable_shared_from_this<LakeSoulDataReader>
{
public:
    LakeSoulDataReader(std::shared_ptr<arrow::Schema> schema,
                       const std::vector<std::string>& file_urls,
                       const std::vector<std::pair<std::string, std::string>>& partition_info);

    int GetBatchSize() const;
    void SetBatchSize(int batch_size);

    int GetThreadNum() const;
    void SetThreadNum(int thread_num);

    void StartReader();
    bool IsFinished() const;
    arrow::Future<std::shared_ptr<arrow::RecordBatch>> ReadRecordBatchAsync();

private:
    lakesoul::IOConfig* CreateIOConfig();
    lakesoul::TokioRuntime* CreateTokioRuntime();
    std::shared_ptr<lakesoul::CResult<lakesoul::Reader>> CreateReader();

    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::string> file_urls_;
    std::vector<std::pair<std::string, std::string>> partition_info_;
    int batch_size_ = 16;
    int thread_num_ = 1;
    std::shared_ptr<lakesoul::CResult<lakesoul::Reader>> reader_;
    bool finished_ = false;
};

} // namespace lakesoul

#endif // LAKESOUL_DATA_READER_H
