// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.jnr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/** The Java DAO codes must match the Rust {@code DaoType} discriminants. */
public class CodedDaoTypeTest {

    @Test
    public void selectByTableIdUsesOneParam() {
        NativeUtils.CodedDaoType type = NativeUtils.CodedDaoType.SelectOneDataCommitInfoByTableId;
        // DataCommitInfoDao.selectByTableId passes a single table id.
        assertEquals(1, type.getParamsNum());
        // Code 13 of the query-one band; the Rust DaoType enum uses +13 for the
        // same DAO (code +10 is SelectTableDomainById there).
        assertEquals(NativeUtils.DAO_TYPE_QUERY_ONE_OFFSET + 13, type.getCode());
    }

    @Test
    public void codesAreUnique() {
        Set<Integer> codes = new HashSet<>();
        for (NativeUtils.CodedDaoType type : NativeUtils.CodedDaoType.values()) {
            assertTrue(
                    "duplicate code " + type.getCode() + " on " + type, codes.add(type.getCode()));
        }
    }
}
