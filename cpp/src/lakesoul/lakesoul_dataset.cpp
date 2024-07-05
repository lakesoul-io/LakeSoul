// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#include <arrow/util/iterator.h>
#include <lakesoul/lakesoul_dataset.h>
#include <lakesoul/lakesoul_fragment.h>

namespace lakesoul {

LakeSoulDataset::LakeSoulDataset(std::shared_ptr<arrow::Schema> schema)
    : arrow::dataset::Dataset(std::move(schema)) { }

// TODO: use partition_expression
LakeSoulDataset::LakeSoulDataset(std::shared_ptr<arrow::Schema> schema,
                                 arrow::compute::Expression partition_expression)
    : arrow::dataset::Dataset(std::move(schema)
    , std::move(partition_expression)) { }

std::string LakeSoulDataset::type_name() const { return "lakesoul"; }

arrow::Result<std::shared_ptr<arrow::dataset::Dataset>>
LakeSoulDataset::ReplaceSchema(std::shared_ptr<arrow::Schema> schema) const
{
    auto dataset = std::make_shared<LakeSoulDataset>(std::move(schema));
    for (const auto& files : file_urls_) {
        dataset->AddFileUrls(files);
    }
    arrow::Result<std::shared_ptr<arrow::dataset::Dataset>> result(std::move(dataset));
    return result;
}

// TODO: use predicate
arrow::Result<arrow::dataset::FragmentIterator>
LakeSoulDataset::GetFragmentsImpl(arrow::compute::Expression predicate)
{
    try {
        std::vector<std::shared_ptr<arrow::dataset::Fragment>> fragments;
        fragments.reserve(file_urls_.size());
        for (size_t i = 0; i < file_urls_.size(); ++i) {
            const auto files = file_urls_.at(i);
            const auto pks = primary_keys_.at(i);
            auto fragment = std::make_shared<LakeSoulFragment>(this->schema());
            fragment->AddFileUrls(files);
            fragment->AddPrimaryKeys(pks);
            fragment->AddPartitionKeyValues(partition_info_);
            fragment->SetBatchSize(batch_size_);
            fragment->SetThreadNum(thread_num_);
            if (retain_partition_columns_) {
                fragment->SetRetainPartitionColumns();
            }
            fragment->SetObjectStoreConfigs(object_store_configs_);
            fragment->CreateDataReader();
            fragments.push_back(fragment);
        }
        fragments_ = fragments;
        arrow::Result<arrow::dataset::FragmentIterator> result(arrow::MakeVectorIterator(std::move(fragments)));
        return result;
    } catch (const std::exception& e) {
        return arrow::Status::IOError(e.what());
    }
}

void LakeSoulDataset::AddFileUrls(const std::vector<std::string>& file_urls)
{
    file_urls_.push_back(file_urls);
}

void LakeSoulDataset::AddPrimaryKeys(const std::vector<std::string>& pks) {
    primary_keys_.push_back(pks);
}

void LakeSoulDataset::AddPartitionKeyValue(const std::string& key, const std::string& value)
{
    partition_info_.push_back(std::make_pair(key, value));
}

void LakeSoulDataset::AddPartitionKeyValues(const std::vector<std::pair<std::string, std::string>>& key_values)
{
    partition_info_.insert(partition_info_.end(), key_values.begin(), key_values.end());
}

int LakeSoulDataset::GetBatchSize() const
{
    return batch_size_;
}

void LakeSoulDataset::SetBatchSize(int batch_size)
{
    batch_size_ = batch_size >= 1 ? batch_size : 1;
}

int LakeSoulDataset::GetThreadNum() const
{
    return thread_num_;
}

void LakeSoulDataset::SetThreadNum(int thread_num)
{
    thread_num_ = thread_num >= 1 ? thread_num : 1;
}

void LakeSoulDataset::SetRetainPartitionColumns() {
    retain_partition_columns_ = true;
}

void LakeSoulDataset::SetObjectStoreConfig(const std::string& key, const std::string& value) {
    object_store_configs_.push_back(std::make_pair(key, value));
}

} // namespace lakesoul
