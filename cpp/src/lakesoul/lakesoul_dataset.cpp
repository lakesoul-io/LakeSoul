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
    dataset->AddFileUrls(file_urls_);
    arrow::Result<std::shared_ptr<arrow::dataset::Dataset>> result(std::move(dataset));
    return result;
}

// TODO: use predicate
arrow::Result<arrow::dataset::FragmentIterator>
LakeSoulDataset::GetFragmentsImpl(arrow::compute::Expression predicate)
{
    auto fragment = std::make_shared<LakeSoulFragment>(this->schema());
    fragment->AddFileUrls(file_urls_);
    fragment->AddPartitionKeyValues(partition_info_);
    fragment->SetBatchSize(batch_size_);
    fragment->SetThreadNum(thread_num_);
    fragment->CreateDataReader();
    std::vector<std::shared_ptr<arrow::dataset::Fragment>> fragments;
    fragments.push_back(fragment);
    arrow::Result<arrow::dataset::FragmentIterator> result(arrow::MakeVectorIterator(std::move(fragments)));
    return result;
}

void LakeSoulDataset::AddFileUrl(const std::string& file_url)
{
    file_urls_.push_back(file_url);
}

void LakeSoulDataset::AddFileUrls(const std::vector<std::string>& file_urls)
{
    file_urls_.insert(file_urls_.end(), file_urls.begin(), file_urls.end());
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

} // namespace lakesoul
