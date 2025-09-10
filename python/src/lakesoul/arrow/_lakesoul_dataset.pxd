# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

# cython: language_level = 3

from libcpp.memory cimport shared_ptr

from pyarrow.includes.libarrow_dataset cimport CDataset
from pyarrow.includes.libarrow_dataset cimport CFragment

from ._lakesoul_dataset_cpp cimport CLakeSoulDataset
from ._lakesoul_dataset_cpp cimport CLakeSoulFragment

from pyarrow._dataset cimport Dataset
from pyarrow._dataset cimport Fragment

cdef class LakeSoulDataset(Dataset):
    cdef:
        CLakeSoulDataset* lakesoul_dataset

    cdef void init(self, const shared_ptr[CDataset]& sp)

cdef class LakeSoulFragment(Fragment):
    cdef:
        CLakeSoulFragment* lakesoul_fragment

    cdef void init(self, const shared_ptr[CFragment]& sp)
