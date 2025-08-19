// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.security;

import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LakeSoulSystemAccessControlFactory implements SystemAccessControlFactory {
    private final String name = "lakesoul";
    private final String principalNeedMatchUsername = "principal.need-match-username";
    private final String sensitiveSystemSessionPropertiesEnableRestrictions = "sensitive.system-session-properties.enable-restrictions";
    private final String sensitiveSystemSessionProperties = "sensitive.system-session-properties";
    private final String sensitiveSystemSessionPropertiesAllowUsername = "sensitive.system-session-properties.allowed-usernames";
    private final String sensitiveSystemSessionPropertiesAllowDomain = "sensitive.system-session-properties.allowed-domains";
    private final String systemSchemas = "system.schemas";

    @Override
    public String getName() {
        return name;
    }

    @Override
    public SystemAccessControl create(Map<String, String> config) {
        boolean principalNeedMatchUsername = Boolean.parseBoolean(
                config.getOrDefault(this.principalNeedMatchUsername, "false"));

        boolean sensitiveSystemSessionPropertiesEnableRestrictions = Boolean.parseBoolean(
                config.getOrDefault(this.sensitiveSystemSessionPropertiesEnableRestrictions, "false"));

        Set<String> sensitiveSystemSessionProperties = Arrays
                .stream(config.getOrDefault(this.sensitiveSystemSessionProperties, "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());

        Set<String> sensitiveSystemSessionPropertiesAllowUsernames = Arrays
                .stream(config.getOrDefault(this.sensitiveSystemSessionPropertiesAllowUsername, "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());

        Set<String> sensitiveSystemSessionPropertiesAllowDomains = Arrays
                .stream(config.getOrDefault(this.sensitiveSystemSessionPropertiesAllowDomain, "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());

        Set<String> systemSchemas = Arrays
                .stream(config.getOrDefault(this.systemSchemas, "information_schema").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());

        return new LakeSoulSystemAccessControl(
                sensitiveSystemSessionProperties,
                principalNeedMatchUsername,
                sensitiveSystemSessionPropertiesEnableRestrictions,
                sensitiveSystemSessionPropertiesAllowUsernames,
                sensitiveSystemSessionPropertiesAllowDomains,
                systemSchemas);
    }
}