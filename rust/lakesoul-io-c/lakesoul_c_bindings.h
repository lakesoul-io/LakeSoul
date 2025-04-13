#ifndef LAKESOUL_C_BINDINGS_H
#define LAKESOUL_C_BINDINGS_H

#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>
#include "stddef.h"

namespace lakesoul {
typedef ptrdiff_t c_ptrdiff_t;
typedef size_t c_size_t;
}

namespace lakesoul {

/// The opaque builder of the IO config.
struct IOConfigBuilder {
  uint8_t private_[0];
};

using c_ptrdiff_t = intptr_t;

using c_size_t = uintptr_t;

/// The opaque IO config.
struct IOConfig {
  uint8_t private_[0];
};

/// The opaque reader.
struct Reader {
  uint8_t private_[0];
};

/// Opaque wrapper for the result of a function call.
template<typename OpaqueT>
struct CResult {
  OpaqueT *ptr;
  const char *err;
};

/// The opaque type for the [`TokioRuntime`].
struct TokioRuntime {
  uint8_t private_[0];
};

/// The callback function with bool result and error string.
using ResultCallback = void(*)(bool, const char*);

/// The callback function with bool result, error string and data pointer.
using DataResultCallback = void(*)(bool, const char*, const void*);

/// The callback function with i32 result and error string.
using I32ResultCallback = void(*)(int32_t, const char*);

/// The callback function with i32 result, error string and data pointer.
using I32DataResultCallback = void(*)(int32_t, const char*, const void*);

/// The opaque writer.
struct Writer {
  uint8_t private_[0];
};

/// The opaque bytes result.
struct BytesResult {
  uint8_t private_[0];
};

/// The opaque type for the [`TokioRuntimeBuilder`].
struct TokioRuntimeBuilder {
  uint8_t private_[0];
};

extern "C" {

/// Create a new [`IOConfigBuilder`].
IOConfigBuilder *new_lakesoul_io_config_builder();

/// Set the prefix of the IO config.
IOConfigBuilder *lakesoul_config_builder_with_prefix(IOConfigBuilder *builder, const char *prefix);

/// Add a single file to the IO config.
IOConfigBuilder *lakesoul_config_builder_add_single_file(IOConfigBuilder *builder,
                                                         const char *file);

/// Add a single column to the IO config.
IOConfigBuilder *lakesoul_config_builder_add_single_column(IOConfigBuilder *builder,
                                                           const char *column);

/// Add a single aux sort column to the IO config.
IOConfigBuilder *lakesoul_config_builder_add_single_aux_sort_column(IOConfigBuilder *builder,
                                                                    const char *column);

/// Add a filter to the IO config.
IOConfigBuilder *lakesoul_config_builder_add_filter(IOConfigBuilder *builder, const char *filter);

/// Add a filter to the IO config from a protobuf.
IOConfigBuilder *lakesoul_config_builder_add_filter_proto(IOConfigBuilder *builder,
                                                          c_ptrdiff_t proto_addr,
                                                          int32_t len);

/// Set the schema of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_schema(IOConfigBuilder *builder,
                                                    c_ptrdiff_t schema_addr);

/// Set the partition schema of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_partition_schema(IOConfigBuilder *builder,
                                                              c_ptrdiff_t schema_addr);

/// Set the thread number of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_thread_num(IOConfigBuilder *builder,
                                                        c_size_t thread_num);

/// Set whether to use dynamic partition of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_dynamic_partition(IOConfigBuilder *builder,
                                                               bool enable);

/// Set whether to infer the schema of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_inferring_schema(IOConfigBuilder *builder,
                                                              bool enable);

/// Set the batch size of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_batch_size(IOConfigBuilder *builder,
                                                        c_size_t batch_size);

/// Set the max row group size of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_max_row_group_size(IOConfigBuilder *builder,
                                                                c_size_t max_row_group_size);

/// Set the max row group num values of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_max_row_group_num_values(IOConfigBuilder *builder,
                                                                      c_size_t max_row_group_num_values);

/// Set the buffer size of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_buffer_size(IOConfigBuilder *builder,
                                                         c_size_t buffer_size);

/// Set the hash bucket number of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_hash_bucket_num(IOConfigBuilder *builder,
                                                             c_size_t hash_bucket_num);

/// Set the object store option of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_object_store_option(IOConfigBuilder *builder,
                                                                 const char *key,
                                                                 const char *value);

/// Add a option to the IO config.
IOConfigBuilder *lakesoul_config_builder_set_option(IOConfigBuilder *builder,
                                                    const char *key,
                                                    const char *value);

/// Add a files to the IO config.
IOConfigBuilder *lakesoul_config_builder_add_files(IOConfigBuilder *builder,
                                                   const char *const *files,
                                                   c_size_t file_num);

/// Add a single primary key to the IO config.
IOConfigBuilder *lakesoul_config_builder_add_single_primary_key(IOConfigBuilder *builder,
                                                                const char *pk);

/// Add a single range partition to the IO config.
IOConfigBuilder *lakesoul_config_builder_add_single_range_partition(IOConfigBuilder *builder,
                                                                    const char *col);

/// Add a merge operation to the IO config.
IOConfigBuilder *lakesoul_config_builder_add_merge_op(IOConfigBuilder *builder,
                                                      const char *field,
                                                      const char *merge_op);

/// Add collection of primary keys to the IO config.
IOConfigBuilder *lakesoul_config_builder_add_primary_keys(IOConfigBuilder *builder,
                                                          const char *const *pks,
                                                          c_size_t pk_num);

/// Set the default column value of the IO config.
IOConfigBuilder *lakesoul_config_builder_set_default_column_value(IOConfigBuilder *builder,
                                                                  const char *field,
                                                                  const char *value);

/// Create a new [`IOConfig`] from the [`IOConfigBuilder`].
IOConfig *create_lakesoul_io_config_from_builder(IOConfigBuilder *builder);

/// Create a new [`SyncSendableMutableLakeSoulReader`] from the [`IOConfig`] and return a [`Reader`] wrapped in [`CResult`].
CResult<Reader> *create_lakesoul_reader_from_config(IOConfig *config,
                                                    TokioRuntime *runtime);

/// Check if the [`Reader`] is created successfully.
const char *check_reader_created(CResult<Reader> *reader);

/// Call [`SyncSendableMutableLakeSoulReader::start_blocked`] of the [`Reader`].
void start_reader(CResult<Reader> *reader, ResultCallback callback);

/// Call [`SyncSendableMutableLakeSoulReader::start_blocked`] of the [`Reader`] with data.
void start_reader_with_data(CResult<Reader> *reader, const void *data, DataResultCallback callback);

/// Call [`SyncSendableMutableLakeSoulReader::next_rb_callback`] of the [`Reader`].
void next_record_batch(CResult<Reader> *reader,
                       c_ptrdiff_t schema_addr,
                       c_ptrdiff_t array_addr,
                       I32ResultCallback callback);

/// Call [`SyncSendableMutableLakeSoulReader::next_rb_blocked`] of the [`Reader`].
const char *next_record_batch_blocked(CResult<Reader> *reader, c_ptrdiff_t array_addr, int *count);

/// Call [`SyncSendableMutableLakeSoulReader::next_rb_callback`] of the [`Reader`].
void next_record_batch_with_data(CResult<Reader> *reader,
                                 c_ptrdiff_t schema_addr,
                                 c_ptrdiff_t array_addr,
                                 const void *data,
                                 I32DataResultCallback callback);

/// Export the schema of the [`Reader`].
void lakesoul_reader_get_schema(CResult<Reader> *reader, c_ptrdiff_t schema_addr);

/// Free the [`Reader`].
void free_lakesoul_reader(CResult<Reader> *reader);

/// Create a new [`SyncSendableMutableLakeSoulWriter`] from the [`IOConfig`] and return a [`Writer`] wrapped in [`CResult`].
CResult<Writer> *create_lakesoul_writer_from_config(IOConfig *config,
                                                    TokioRuntime *runtime);

const char *check_writer_created(CResult<Reader> *writer);

/// Call [`SyncSendableMutableLakeSoulWriter::write_batch`] of the [`Writer`] with callback.
void write_record_batch(CResult<Writer> *writer,
                        c_ptrdiff_t schema_addr,
                        c_ptrdiff_t array_addr,
                        ResultCallback callback);

/// Call [`SyncSendableMutableLakeSoulWriter::write_batch`] of the [`Writer`] by blocking mode.
const char *write_record_batch_blocked(CResult<Writer> *writer,
                                       c_ptrdiff_t schema_addr,
                                       c_ptrdiff_t array_addr);

/// Call [`SyncSendableMutableLakeSoulWriter::write_batch`] of the [`Writer`] by blocking mode, record batch is read from ipc protocol.
const char *write_record_batch_ipc_blocked(CResult<Writer> *writer,
                                           c_ptrdiff_t ipc_addr,
                                           int64_t len);

/// Export the bytes result of the [`Writer`].
void export_bytes_result(void (*callback)(bool, const char*),
                         CResult<BytesResult> *bytes,
                         int32_t len,
                         c_ptrdiff_t addr);

/// Flush and close the [`Writer`] and return the [`BytesResult`] wrapped in [`CResult`].
CResult<BytesResult> *flush_and_close_writer(CResult<Writer> *writer, I32ResultCallback callback);

/// Abort and close the [`Writer`] and return the [`BytesResult`] wrapped in [`CResult`], when encountering an external error.
void abort_and_close_writer(CResult<Writer> *writer,
                            ResultCallback callback);

/// Create a new [`TokioRuntimeBuilder`].
TokioRuntimeBuilder *new_tokio_runtime_builder();

/// Set the number of threads of the [`TokioRuntimeBuilder`].
TokioRuntimeBuilder *tokio_runtime_builder_set_thread_num(TokioRuntimeBuilder *builder,
                                                          c_size_t thread_num);

/// Create a new [`TokioRuntime`] from the [`TokioRuntimeBuilder`].
TokioRuntime *create_tokio_runtime_from_builder(TokioRuntimeBuilder *builder);

void free_tokio_runtime(CResult<TokioRuntime> *runtime);

/// Apply the partition filter to the [`entity::JniWrapper`] and return the [`BytesResult`] wrapped in [`CResult`].
CResult<BytesResult> *apply_partition_filter(void (*callback)(int32_t, const char*),
                                             int32_t len,
                                             c_ptrdiff_t jni_wrapper_addr,
                                             c_ptrdiff_t schema_addr,
                                             int32_t filter_len,
                                             c_ptrdiff_t filter_addr);

/// Free the [`BytesResult`].
void free_bytes_result(CResult<BytesResult> *bytes);

void rust_logger_init();

} // extern "C"

} // namespace lakesoul

#endif // LAKESOUL_C_BINDINGS_H
