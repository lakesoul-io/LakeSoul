// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#ifndef LAKESOUL_DATASET_H
#define LAKESOUL_DATASET_H

#include <arrow/dataset/dataset.h>

namespace lakesoul {

class LakeSoulDataset : public arrow::dataset::Dataset
{
public:
    explicit LakeSoulDataset(std::shared_ptr<arrow::Schema> schema);

    LakeSoulDataset(std::shared_ptr<arrow::Schema> schema,
                    arrow::compute::Expression partition_expression);

    std::string type_name() const override;

    arrow::Result<std::shared_ptr<arrow::dataset::Dataset>>
    ReplaceSchema(std::shared_ptr<arrow::Schema> schema) const override;

    arrow::Result<arrow::dataset::FragmentIterator>
    GetFragmentsImpl(arrow::compute::Expression predicate) override;

    void AddFileUrls(const std::vector<std::string>& file_urls);

    void AddPrimaryKeys(const std::vector<std::string>& pks);

    void AddPartitionKeyValue(const std::string& key, const std::string& value);
    void AddPartitionKeyValues(const std::vector<std::pair<std::string, std::string>>& key_values);

    int GetBatchSize() const;
    void SetBatchSize(int batch_size);

    int GetThreadNum() const;
    void SetThreadNum(int thread_num);

    void SetRetainPartitionColumns();

    void SetObjectStoreConfig(const std::string& key, const std::string& value);

private:
    std::vector<std::vector<std::string>> file_urls_;
    std::vector<std::vector<std::string>> primary_keys_;
    std::vector<std::pair<std::string, std::string>> partition_info_;
    std::vector<std::shared_ptr<arrow::dataset::Fragment>> fragments_;
    std::vector<std::pair<std::string, std::string>> object_store_configs_;
    int batch_size_ = 16;
    int thread_num_ = 1;
    bool retain_partition_columns_ = false;
};

} // namespace lakesoul

#endif // LAKESOUL_DATASET_H
