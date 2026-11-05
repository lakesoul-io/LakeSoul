// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.security;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.dmetasoul.lakesoul.meta.security.Claims;
import com.facebook.presto.spi.security.AccessDeniedException;

import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Test(singleThreaded = true)
public class LakesoulPrestoAuthenticatorTest {
    private static final String TOKEN_SENTINEL = "secret-token-sentinel";

    private final LakesoulPrestoAuthenticator authenticator = new LakesoulPrestoAuthenticator();

    public void missingHeaderIsReported() {
        try {
            authenticator.createAuthenticatedPrincipal(Collections.emptyMap(), token -> null);
            fail("missing header must be rejected");
        } catch (AccessDeniedException error) {
            assertTrue(error.getMessage().contains("Authorization header is missing!"));
        }
    }

    public void malformedAuthorizationHeaderNeverEchoesItsValue() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put("Authorization", Collections.singletonList("Basic " + TOKEN_SENTINEL));

        try {
            authenticator.createAuthenticatedPrincipal(headers, token -> null);
            fail("a non-bearer header must be rejected");
        } catch (AccessDeniedException error) {
            assertTrue(
                    error.getMessage()
                            .contains("Authorization header format must be Bearer <token>"));
            assertFalse(error.getMessage().contains(TOKEN_SENTINEL));
        }
    }

    public void invalidTokenNeverEchoesItsValue() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put("Authorization", Collections.singletonList("Bearer " + TOKEN_SENTINEL));

        try {
            authenticator.createAuthenticatedPrincipal(headers, token -> null);
            fail("an undecodable token must be rejected");
        } catch (AccessDeniedException error) {
            assertTrue(error.getMessage().contains("Invalid token"));
            assertFalse(error.getMessage().contains(TOKEN_SENTINEL));
        }
    }

    public void validTokenKeepsItsExactValueAndClaims() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put("Authorization", Collections.singletonList("Bearer " + TOKEN_SENTINEL));
        Claims claims = new Claims();
        claims.setSub("alice");
        claims.setGroup("analysts");

        LakeSoulAuthenticatedPrincipal principal =
                (LakeSoulAuthenticatedPrincipal)
                        authenticator.createAuthenticatedPrincipal(
                                headers,
                                token -> {
                                    assertTrue(TOKEN_SENTINEL.equals(token));
                                    return claims;
                                });

        assertEquals(principal.getSub(), "alice");
        assertEquals(principal.getGroup(), "analysts");
    }
}
