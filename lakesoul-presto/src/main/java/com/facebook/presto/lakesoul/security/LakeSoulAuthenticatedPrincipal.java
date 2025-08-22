// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.security;

import java.security.Principal;
import java.time.Instant;

import com.dmetasoul.lakesoul.meta.security.Claims;

public class LakeSoulAuthenticatedPrincipal implements Principal {
    private static final String name = "lakesoul";
    private final String sub;
    private final String group;
    private final Instant exp;

    public LakeSoulAuthenticatedPrincipal(String sub, String group, Instant exp) {
        this.sub = sub;
        this.group = group;
        this.exp = exp;
    }

    public LakeSoulAuthenticatedPrincipal(Claims claims) {
        this.sub = claims.getSub();
        this.group = claims.getGroup();
        this.exp = Instant.ofEpochMilli(claims.getExp());
    }

    public String getSub() {
        return sub;
    }

    public String getGroup() {
        return group;
    }

    public Instant getExp() {
        return exp;
    }

    public boolean isExpired() {
        return exp.isBefore(Instant.now());
    }

    @Override
    public String getName() {
        return name;
    }
}
