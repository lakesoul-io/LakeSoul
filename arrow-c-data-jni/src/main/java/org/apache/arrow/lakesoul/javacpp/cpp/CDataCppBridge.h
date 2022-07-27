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
//    std::shared_ptr<arrow::Schema> schema =
//            arrow::schema({arrow::field("x", arrow::int64())});
//    auto c_schema_status = arrow::ExportSchema(*schema, c_schema);

    if (!c_schema_status.ok()) c_schema_status.Abort();

    struct ArrowArray* c_array = reinterpret_cast<struct ArrowArray*>(c_array_ptr);
    auto c_array_status = arrow::ExportArray(*array, c_array);
    if (!c_array_status.ok()) c_array_status.Abort();
}

class Foo {
public:
    int n;
    Foo(int n) : n(n) { }
    virtual ~Foo() { }
    virtual void bar() {
        printf("Callback in C++ (n == %d)\n", n);
    }

};

void FillInt64ArrayWithCallBack(const uintptr_t c_schema_ptr, const uintptr_t c_array_ptr, Foo *foo) {
    foo->bar();
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
//    std::shared_ptr<arrow::Schema> schema =
//            arrow::schema({arrow::field("x", arrow::int64())});
//    auto c_schema_status = arrow::ExportSchema(*schema, c_schema);

    if (!c_schema_status.ok()) c_schema_status.Abort();

    struct ArrowArray* c_array = reinterpret_cast<struct ArrowArray*>(c_array_ptr);
    auto c_array_status = arrow::ExportArray(*array, c_array);
    if (!c_array_status.ok()) c_array_status.Abort();


}

void FillInt64Batch(const uintptr_t c_schema_ptr, const uintptr_t c_array_ptr) {
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

    std::shared_ptr<arrow::Schema> schema = arrow::schema({arrow::field("x", arrow::int64())});

    std::vector<std::shared_ptr<arrow::Array>> arrays_;

    arrays_.push_back(array);
    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema, 10, arrays_);

    struct ArrowSchema* c_schema = reinterpret_cast<struct ArrowSchema*>(c_schema_ptr);
    struct ArrowArray* c_array = reinterpret_cast<struct ArrowArray*>(c_array_ptr);

    auto c_batch_status = arrow::ExportRecordBatch(*batch, c_array, c_schema);
    if (!c_batch_status.ok()) c_batch_status.Abort();



}

