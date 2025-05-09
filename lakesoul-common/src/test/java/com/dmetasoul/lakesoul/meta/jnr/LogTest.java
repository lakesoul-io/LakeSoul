/*
 * SPDX-FileCopyrightText: 2025 LakeSoul Contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.dmetasoul.lakesoul.meta.jnr;

public class LogTest {
    public static void main(String[] args) {
        LibLakeSoulMetaData lib =  JnrLoader.get();
//        lib.rust_logger_init();
        lib.rust_logger_init();
        lib.rust_logger_warn();
        lib.rust_logger_error();
    }
}
