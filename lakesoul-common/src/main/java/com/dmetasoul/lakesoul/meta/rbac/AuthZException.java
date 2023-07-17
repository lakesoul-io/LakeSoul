// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac;

public class AuthZException extends RuntimeException {
    public AuthZException(){
        super("lakesoul access denied!");
    }
}
