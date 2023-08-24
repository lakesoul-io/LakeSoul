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

    void AddFileUrl(const std::string& file_url);
    void AddFileUrls(const std::vector<std::string>& file_urls);

private:
    std::vector<std::string> file_urls_;
};

} // namespace lakesoul

#endif // LAKESOUL_DATASET_H
