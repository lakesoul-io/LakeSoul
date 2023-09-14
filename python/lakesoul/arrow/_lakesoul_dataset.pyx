# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

# cython: language_level = 3

from libcpp.string cimport string
from libcpp.memory cimport shared_ptr
from libcpp.memory cimport make_shared

from pyarrow.lib cimport Schema
from pyarrow.includes.common cimport move
from pyarrow.includes.common cimport GetResultValue
from pyarrow.includes.libarrow cimport CExpression
from pyarrow._compute cimport _bind
from pyarrow._compute cimport Expression

cdef class LakeSoulDataset(Dataset):
    def __init__(self, Schema schema):
        cdef:
            shared_ptr[CLakeSoulDataset] lakesoul_dataset
        lakesoul_dataset = make_shared[CLakeSoulDataset](schema.sp_schema)
        self.init(<shared_ptr[CDataset]> lakesoul_dataset)

    cdef void init(self, const shared_ptr[CDataset]& sp):
        Dataset.init(self, sp)
        self.lakesoul_dataset = <CLakeSoulDataset*> sp.get()

    # The method '_get_fragments' in the base class 'Dataset' does not
    # deal with 'LakeSoulFragment', override it.
    def _get_fragments(self, Expression filter):
        cdef:
            CExpression c_filter

        if filter is None:
            c_fragments = move(GetResultValue(self.dataset.GetFragments()))
        else:
            c_filter = _bind(filter, self.schema)
            c_fragments = move(GetResultValue(
                self.dataset.GetFragments(c_filter)))

        for maybe_fragment in c_fragments:
            yield LakeSoulFragment.wrap(GetResultValue(move(maybe_fragment)))

    def _add_file_url(self, file_url):
        cdef string cpp_string = file_url.encode('utf-8')
        self.lakesoul_dataset.AddFileUrl(cpp_string)

    def _add_partition_key_value(self, key, value):
        cdef string key_cpp_string = key.encode('utf-8')
        cdef string value_cpp_string = value.encode('utf-8')
        self.lakesoul_dataset.AddPartitionKeyValue(key_cpp_string, value_cpp_string)

    def _set_batch_size(self, batch_size):
        self.lakesoul_dataset.SetBatchSize(batch_size)

    def _set_thread_num(self, thread_num):
        self.lakesoul_dataset.SetThreadNum(thread_num)

    def scanner(self, *args, **kwargs):
        # LakeSoul already uses threads, moreover using Arrow threads
        # will make LakeSoulDataReader being destructed in an asynchronous
        # context, so we set use_threads to False to avoid Arrow threads.
        kwargs['use_threads'] = False
        return super().scanner(*args, **kwargs)

cdef class LakeSoulFragment(Fragment):
    cdef void init(self, const shared_ptr[CFragment]& sp):
        Fragment.init(self, sp)
        self.lakesoul_fragment = <CLakeSoulFragment*> sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CFragment]& sp):
        cdef LakeSoulFragment self = LakeSoulFragment.__new__(LakeSoulFragment)
        self.init(sp)
        return self

    def scanner(self, *args, **kwargs):
        # LakeSoul already uses threads, moreover using Arrow threads
        # will make LakeSoulDataReader being destructed in an asynchronous
        # context, so we set use_threads to False to avoid Arrow threads.
        kwargs['use_threads'] = False
        return super().scanner(*args, **kwargs)
