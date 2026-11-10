// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.security;

import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;

public class JwtUtils {
    public static Claims parseClaims(String token) {
        return NativeMetadataJavaClient.getInstance().decodeTokenToClaims(token);
    }

    public static String encodeClaims(Claims claims) {
        return NativeMetadataJavaClient.getInstance().encodeTokenFromClaims(claims);
    }
}
