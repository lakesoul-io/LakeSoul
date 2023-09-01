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

} // namespace lakesoul
