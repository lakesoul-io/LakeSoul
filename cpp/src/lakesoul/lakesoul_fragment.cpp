// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#include <lakesoul/lakesoul_fragment.h>

namespace lakesoul {

LakeSoulFragment::LakeSoulFragment(std::shared_ptr<arrow::Schema> schema)
    : schema_(std::move(schema))
{
}

arrow::Result<arrow::dataset::RecordBatchGenerator>
LakeSoulFragment::ScanBatchesAsync(const std::shared_ptr<arrow::dataset::ScanOptions>& options)
{
    auto fragment = shared_from_this();
    arrow::dataset::RecordBatchGenerator gen = [fragment = fragment] {
        auto frag = std::static_pointer_cast<LakeSoulFragment>(fragment);
        if (frag->data_reader_->IsFinished())
            frag->CreateDataReader();
        return frag->data_reader_->ReadRecordBatchAsync();
    };
    arrow::Result<arrow::dataset::RecordBatchGenerator> result(std::move(gen));
    return result;
}

std::string LakeSoulFragment::type_name() const
{
    return "lakesoul";
}

arrow::Result<std::shared_ptr<arrow::Schema>>
LakeSoulFragment::ReadPhysicalSchemaImpl()
{
    arrow::Result<std::shared_ptr<arrow::Schema>> result(schema_);
    return result;
}

void LakeSoulFragment::AddFileUrl(const std::string& file_url)
{
    file_urls_.push_back(file_url);
}

void LakeSoulFragment::AddFileUrls(const std::vector<std::string>& file_urls)
{
    file_urls_.insert(file_urls_.end(), file_urls.begin(), file_urls.end());
}

void LakeSoulFragment::CreateDataReader()
{
    data_reader_ = std::make_shared<lakesoul::LakeSoulDataReader>(schema_, file_urls_);
    data_reader_->StartReader();
}

} // namespace lakesoul
