// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.security;

import java.security.Principal;
import java.time.Instant;

import com.dmetasoul.lakesoul.meta.security.Claims;
import com.facebook.presto.jdbc.internal.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LakeSoulAuthenticatedPrincipal implements Principal {
    private static final String name = "lakesoul";
    private final String sub;
    private final String group;
    private final Instant exp;

    @JsonCreator
    public LakeSoulAuthenticatedPrincipal(
            @JsonProperty("sub") String sub,
            @JsonProperty("group") String group,
            @JsonProperty("exp") Instant exp) {
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

    @JsonIgnore
    public boolean isExpired() {
        return exp.isBefore(Instant.now());
    }

    @JsonIgnore
    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize LakeSoulAuthenticatedPrincipal", e);
        }
    }
}
