#include <iostream>
#include <arrow/api.h>
#include <arrow/c/bridge.h>

void FillInt64Array(const uintptr_t c_schema_ptr, const uintptr_t c_array_ptr) {
    arrow::Int64Builder builder;
    builder.Append(1);
    builder.Append(2);
    builder.Append(3);
    builder.AppendNull();
    builder.Append(5);
    builder.Append(6);
    builder.Append(7);
    builder.Append(8);
    builder.Append(9);
    builder.Append(10);
    std::shared_ptr<arrow::Array> array = *builder.Finish();

    struct ArrowSchema* c_schema = reinterpret_cast<struct ArrowSchema*>(c_schema_ptr);
    auto c_schema_status = arrow::ExportType(*array->type(), c_schema);
    if (!c_schema_status.ok()) c_schema_status.Abort();

    struct ArrowArray* c_array = reinterpret_cast<struct ArrowArray*>(c_array_ptr);
    auto c_array_status = arrow::ExportArray(*array, c_array);
    if (!c_array_status.ok()) c_array_status.Abort();
}
