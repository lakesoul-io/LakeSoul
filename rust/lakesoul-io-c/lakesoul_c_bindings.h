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

struct IOConfigBuilder {
  uint8_t private_[0];
};

using c_ptrdiff_t = intptr_t;

using c_size_t = uintptr_t;

struct IOConfig {
  uint8_t private_[0];
};

struct Reader {
  uint8_t private_[0];
};

template<typename OpaqueT>
struct CResult {
  OpaqueT *ptr;
  const char *err;
};

struct TokioRuntime {
  uint8_t private_[0];
};

using ResultCallback = void(*)(bool, const char*);

using DataResultCallback = void(*)(bool, const char*, const void*);

using I32ResultCallback = void(*)(int32_t, const char*);

using I32DataResultCallback = void(*)(int32_t, const char*, const void*);

struct Writer {
  uint8_t private_[0];
};

struct BytesResult {
  uint8_t private_[0];
};

struct TokioRuntimeBuilder {
  uint8_t private_[0];
};

extern "C" {

IOConfigBuilder *new_lakesoul_io_config_builder();

IOConfigBuilder *lakesoul_config_builder_with_prefix(IOConfigBuilder *builder, const char *prefix);

IOConfigBuilder *lakesoul_config_builder_add_single_file(IOConfigBuilder *builder,
                                                         const char *file);

IOConfigBuilder *lakesoul_config_builder_add_single_column(IOConfigBuilder *builder,
                                                           const char *column);

IOConfigBuilder *lakesoul_config_builder_add_single_aux_sort_column(IOConfigBuilder *builder,
                                                                    const char *column);

IOConfigBuilder *lakesoul_config_builder_add_filter(IOConfigBuilder *builder, const char *filter);

IOConfigBuilder *lakesoul_config_builder_add_filter_proto(IOConfigBuilder *builder,
                                                          c_ptrdiff_t proto_addr,
                                                          int32_t len);

IOConfigBuilder *lakesoul_config_builder_set_schema(IOConfigBuilder *builder,
                                                    c_ptrdiff_t schema_addr);

IOConfigBuilder *lakesoul_config_builder_set_partition_schema(IOConfigBuilder *builder,
                                                              c_ptrdiff_t schema_addr);

IOConfigBuilder *lakesoul_config_builder_set_thread_num(IOConfigBuilder *builder,
                                                        c_size_t thread_num);

IOConfigBuilder *lakesoul_config_builder_set_dynamic_partition(IOConfigBuilder *builder,
                                                               bool enable);

IOConfigBuilder *lakesoul_config_builder_set_inferring_schema(IOConfigBuilder *builder,
                                                              bool enable);

IOConfigBuilder *lakesoul_config_builder_set_batch_size(IOConfigBuilder *builder,
                                                        c_size_t batch_size);

IOConfigBuilder *lakesoul_config_builder_set_max_row_group_size(IOConfigBuilder *builder,
                                                                c_size_t max_row_group_size);

IOConfigBuilder *lakesoul_config_builder_set_max_row_group_num_values(IOConfigBuilder *builder,
                                                                      c_size_t max_row_group_num_values);

IOConfigBuilder *lakesoul_config_builder_set_buffer_size(IOConfigBuilder *builder,
                                                         c_size_t buffer_size);

IOConfigBuilder *lakesoul_config_builder_set_hash_bucket_num(IOConfigBuilder *builder,
                                                             c_size_t hash_bucket_num);

IOConfigBuilder *lakesoul_config_builder_set_object_store_option(IOConfigBuilder *builder,
                                                                 const char *key,
                                                                 const char *value);

IOConfigBuilder *lakesoul_config_builder_set_option(IOConfigBuilder *builder,
                                                    const char *key,
                                                    const char *value);

IOConfigBuilder *lakesoul_config_builder_add_files(IOConfigBuilder *builder,
                                                   const char *const *files,
                                                   c_size_t file_num);

IOConfigBuilder *lakesoul_config_builder_add_single_primary_key(IOConfigBuilder *builder,
                                                                const char *pk);

IOConfigBuilder *lakesoul_config_builder_add_single_range_partition(IOConfigBuilder *builder,
                                                                    const char *col);

IOConfigBuilder *lakesoul_config_builder_add_merge_op(IOConfigBuilder *builder,
                                                      const char *field,
                                                      const char *merge_op);

IOConfigBuilder *lakesoul_config_builder_add_primary_keys(IOConfigBuilder *builder,
                                                          const char *const *pks,
                                                          c_size_t pk_num);

IOConfigBuilder *lakesoul_config_builder_set_default_column_value(IOConfigBuilder *builder,
                                                                  const char *field,
                                                                  const char *value);

IOConfig *create_lakesoul_io_config_from_builder(IOConfigBuilder *builder);

CResult<Reader> *create_lakesoul_reader_from_config(IOConfig *config, TokioRuntime *runtime);

const char *check_reader_created(CResult<Reader> *reader);

void start_reader(CResult<Reader> *reader, ResultCallback callback);

void start_reader_with_data(CResult<Reader> *reader, const void *data, DataResultCallback callback);

void next_record_batch(CResult<Reader> *reader,
                       c_ptrdiff_t schema_addr,
                       c_ptrdiff_t array_addr,
                       I32ResultCallback callback);

const char *next_record_batch_blocked(CResult<Reader> *reader, c_ptrdiff_t array_addr, int *count);

void next_record_batch_with_data(CResult<Reader> *reader,
                                 c_ptrdiff_t schema_addr,
                                 c_ptrdiff_t array_addr,
                                 const void *data,
                                 I32DataResultCallback callback);

void lakesoul_reader_get_schema(CResult<Reader> *reader, c_ptrdiff_t schema_addr);

void free_lakesoul_reader(CResult<Reader> *reader);

CResult<Writer> *create_lakesoul_writer_from_config(IOConfig *config, TokioRuntime *runtime);

const char *check_writer_created(CResult<Reader> *writer);

void write_record_batch(CResult<Writer> *writer,
                        c_ptrdiff_t schema_addr,
                        c_ptrdiff_t array_addr,
                        ResultCallback callback);

const char *write_record_batch_blocked(CResult<Writer> *writer,
                                       c_ptrdiff_t schema_addr,
                                       c_ptrdiff_t array_addr);

const char *write_record_batch_ipc_blocked(CResult<Writer> *writer,
                                           c_ptrdiff_t ipc_addr,
                                           int64_t len);

void export_bytes_result(void (*callback)(bool, const char*),
                         CResult<BytesResult> *bytes,
                         int32_t len,
                         c_ptrdiff_t addr);

CResult<BytesResult> *flush_and_close_writer(CResult<Writer> *writer, I32ResultCallback callback);

void abort_and_close_writer(CResult<Writer> *writer, ResultCallback callback);

TokioRuntimeBuilder *new_tokio_runtime_builder();

TokioRuntimeBuilder *tokio_runtime_builder_set_thread_num(TokioRuntimeBuilder *builder,
                                                          c_size_t thread_num);

TokioRuntime *create_tokio_runtime_from_builder(TokioRuntimeBuilder *builder);

void free_tokio_runtime(CResult<TokioRuntime> *runtime);

CResult<BytesResult> *apply_partition_filter(void (*callback)(int32_t, const char*),
                                             int32_t len,
                                             c_ptrdiff_t jni_wrapper_addr,
                                             c_ptrdiff_t schema_addr,
                                             int32_t filter_len,
                                             c_ptrdiff_t filter_addr);

void free_bytes_result(CResult<BytesResult> *bytes);

void rust_logger_init();

} // extern "C"

} // namespace lakesoul

#endif // LAKESOUL_C_BINDINGS_H
