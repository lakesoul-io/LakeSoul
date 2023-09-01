// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#ifndef LAKESOUL_FRAGMENT_H
#define LAKESOUL_FRAGMENT_H

#include <arrow/dataset/dataset.h>
#include <lakesoul/lakesoul_data_reader.h>

namespace lakesoul {

class LakeSoulFragment : public arrow::dataset::Fragment
{
public:
    explicit LakeSoulFragment(std::shared_ptr<arrow::Schema> schema);

    arrow::Result<arrow::dataset::RecordBatchGenerator>
    ScanBatchesAsync(const std::shared_ptr<arrow::dataset::ScanOptions>& options) override;

    std::string type_name() const override;

    arrow::Result<std::shared_ptr<arrow::Schema>>
    ReadPhysicalSchemaImpl() override;

    void AddFileUrl(const std::string& file_url);
    void AddFileUrls(const std::vector<std::string>& file_urls);
    void CreateDataReader();

private:
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::string> file_urls_;
    std::shared_ptr<lakesoul::LakeSoulDataReader> data_reader_;
};

} // namespace lakesoul

#endif // LAKESOUL_FRAGMENT_H
