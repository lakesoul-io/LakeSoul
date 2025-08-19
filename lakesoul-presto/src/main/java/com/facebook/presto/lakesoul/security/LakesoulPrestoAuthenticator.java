// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.security;

import com.dmetasoul.lakesoul.meta.security.Claims;
import com.dmetasoul.lakesoul.meta.security.JwtUtils;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.PrestoAuthenticator;

import java.security.Principal;
import java.util.List;
import java.util.Map;

public class LakesoulPrestoAuthenticator implements PrestoAuthenticator {
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    @Override
    public Principal createAuthenticatedPrincipal(Map<String, List<String>> headers) {
        if (!headers.containsKey(AUTHORIZATION_HEADER)) {
            throw new AccessDeniedException("Authorization header is missing!");
        } else if (!(headers.get(AUTHORIZATION_HEADER).size() >= 1
                && headers.get(AUTHORIZATION_HEADER).get(0).startsWith(BEARER_PREFIX))) {
            throw new AccessDeniedException(
                    "Authorization header format must be Bearer <token>, but is " + headers.get(AUTHORIZATION_HEADER));
        }

        String token = headers.get(AUTHORIZATION_HEADER).get(0).substring(BEARER_PREFIX.length());
        Claims claims = JwtUtils.parseClaims(token);
        if (claims == null) {
            throw new AccessDeniedException("Invalid token: " + token);
        }

        return new LakeSoulAuthenticatedPrincipal(claims);
    }
}
