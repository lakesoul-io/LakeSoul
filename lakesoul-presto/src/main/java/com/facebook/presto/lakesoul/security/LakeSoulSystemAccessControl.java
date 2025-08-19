// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.security;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.NamespaceTableName;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.stream.Collectors;

public class LakeSoulSystemAccessControl implements SystemAccessControl {
    private static final Logger log = Logger.get(LakeSoulSystemAccessControl.class);
    private static final DBManager dbManager = new DBManager();
    private static final String publicDomain = "public";

    private final Set<String> sensitiveSystemSessionProperties;
    private final boolean principalNeedMatchUsername;
    private final boolean sensitiveSystemSessionPropertiesEnableRestrictions;
    private final Set<String> sensitiveSystemSessionPropertiesAllowUsernames;
    private final Set<String> sensitiveSystemSessionPropertiesAllowDomains;
    private final Set<String> systemSchemas;

    public LakeSoulSystemAccessControl(
            Set<String> sensitiveSystemSessionProperties,
            boolean principalNeedMatchUsername,
            boolean sensitiveSystemSessionPropertiesEnableRestrictions,
            Set<String> sensitiveSystemSessionPropertiesAllowUsernames,
            Set<String> sensitiveSystemSessionPropertiesAllowDomains,
            Set<String> systemSchemas) {
        this.sensitiveSystemSessionProperties = sensitiveSystemSessionProperties;
        this.principalNeedMatchUsername = principalNeedMatchUsername;
        this.sensitiveSystemSessionPropertiesEnableRestrictions = sensitiveSystemSessionPropertiesEnableRestrictions;
        this.sensitiveSystemSessionPropertiesAllowUsernames = sensitiveSystemSessionPropertiesAllowUsernames;
        this.sensitiveSystemSessionPropertiesAllowDomains = sensitiveSystemSessionPropertiesAllowDomains;
        this.systemSchemas = systemSchemas;
    }

    private Optional<LakeSoulAuthenticatedPrincipal> getPrincipal(Identity identity) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        String jsonString = identity.getPrincipal().map(Principal::toString).orElse(null);
        if (jsonString == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(jsonString, LakeSoulAuthenticatedPrincipal.class));
        } catch (JsonProcessingException e) {
            log.error("getPrincipal error: %s", e);
            return Optional.empty();
        }
    }

    @Override
    public void checkCanSetUser(Identity identity, AccessControlContext context, Optional<Principal> principal,
            String userName) {
        if (principalNeedMatchUsername) {
            if (getPrincipal(identity).isPresent()) {
                LakeSoulAuthenticatedPrincipal lakesoulPrincipal = getPrincipal(identity).get();
                if (!lakesoulPrincipal.isExpired()) {
                    String sub = lakesoulPrincipal.getSub();
                    if (!sub.equals(userName)) {
                        throw new AccessDeniedException(
                                String.format(
                                        "Access denied: principal <group: '%s', sub: '%s'> does not match requested user name '%s'.",
                                        lakesoulPrincipal.getGroup(), lakesoulPrincipal.getSub(), userName));
                    }
                } else {
                    throw new AccessDeniedException(
                            String.format("Access denied: principal <group: '%s', sub: '%s'> is expired.",
                                    lakesoulPrincipal.getGroup(), lakesoulPrincipal.getSub()));
                }
            } else {
                throw new AccessDeniedException(
                        "Access denied: no principal information available to verify requested user name.");
            }
        }
    }

    @Override
    public AuthorizedIdentity selectAuthorizedIdentity(Identity identity, AccessControlContext context, String userName,
            List<X509Certificate> certificates) {
        return SystemAccessControl.super.selectAuthorizedIdentity(identity, context, userName, certificates);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, AccessControlContext context, String propertyName) {
        if (sensitiveSystemSessionPropertiesEnableRestrictions
                && sensitiveSystemSessionProperties.contains(propertyName)) {
            if (getPrincipal(identity).isPresent()) {
                LakeSoulAuthenticatedPrincipal lakesoulPrincipal = getPrincipal(identity).get();
                if (!lakesoulPrincipal.isExpired()) {
                    String username = lakesoulPrincipal.getSub();
                    String domain = lakesoulPrincipal.getGroup();
                    if (!sensitiveSystemSessionPropertiesAllowUsernames.contains(username)
                            && !sensitiveSystemSessionPropertiesAllowDomains.contains(domain)) {
                        throw new AccessDeniedException(
                                String.format(
                                        "Access denied: principal <group: '%s', sub: '%s'> is not allowed to set system session property '%s'.",
                                        username, domain, propertyName));
                    }
                    return;
                }
            }
            if (!sensitiveSystemSessionPropertiesAllowUsernames.contains(identity.getUser())) {
                throw new AccessDeniedException(
                        String.format("Access denied: user '%s' is not allowed to set system session property '%s'.",
                                identity.getUser(), propertyName));
            }
        }
    }

    // ===== For catalogs, we pass all of them. =====

    @Override
    public void checkCanAccessCatalog(Identity identity, AccessControlContext context, String catalogName) {
        // no-op
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, AccessControlContext context, Set<String> catalogs) {
        return catalogs;
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, AccessControlContext context, String catalogName,
            String propertyName) {
        // no-op
    }

    // ===== For schemas, we filter them by domain. =====

    @Override
    public Set<String> filterSchemas(Identity identity, AccessControlContext context, String catalogName,
            Set<String> schemaNames) {
        Set<String> filteredSchemas = new HashSet<>(schemaNames);
        List<String> publicAndSystemNamespaces = new ArrayList<>(dbManager.listNamespacesByDomain(publicDomain));
        publicAndSystemNamespaces.addAll(systemSchemas);
        if (getPrincipal(identity).isPresent()) {
            LakeSoulAuthenticatedPrincipal lakesoulPrincipal = getPrincipal(identity).get();

            if (!lakesoulPrincipal.isExpired()) {
                String domain = lakesoulPrincipal.getGroup();
                List<String> namespaces = new ArrayList<>(publicAndSystemNamespaces);
                if (!domain.equals(publicDomain)) {
                    namespaces.addAll(dbManager.listNamespacesByDomain(domain));
                }
                filteredSchemas.retainAll(namespaces);
                log.info("Filter schemas by domain <%s + public>, original schemas: %s, filtered schemas: %s",
                        domain, schemaNames, filteredSchemas);
                return filteredSchemas;
            }
        }
        filteredSchemas.retainAll(publicAndSystemNamespaces);
        log.info("Filter schemas by domain <public>, original schemas: %s, filtered schemas: %s",
                schemaNames, filteredSchemas);
        return filteredSchemas;
    }

    @Override
    public void checkCanShowSchemas(Identity identity, AccessControlContext context, String catalogName) {
        // no-op
    }

    @Override
    public void checkCanCreateSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema) {
        // no-op
    }

    @Override
    public void checkCanDropSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema) {
        // no-op
    }

    @Override
    public void checkCanRenameSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema,
            String newSchemaName) {
        // no-op
    }

    // ===== For tables, we filter them by domain. =====

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, AccessControlContext context, String catalogName,
            Set<SchemaTableName> tableNames) {
        Set<SchemaTableName> filteredTableNames = new HashSet<>(tableNames);
        List<NamespaceTableName> publicTables = dbManager.listTableNamesByDomain(publicDomain);
        if (getPrincipal(identity).isPresent()) {
            LakeSoulAuthenticatedPrincipal lakesoulPrincipal = getPrincipal(identity).get();

            if (!lakesoulPrincipal.isExpired()) {
                String domain = lakesoulPrincipal.getGroup();
                List<NamespaceTableName> tables = new ArrayList<>(publicTables);
                if (!domain.equals(publicDomain)) {
                    tables.addAll(dbManager.listTableNamesByDomain(domain));
                }
                List<SchemaTableName> schemaTableNames = tables.stream().map(e -> {
                    return new SchemaTableName(e.getNamespace(), e.getTableName());
                }).collect(Collectors.toList());
                filteredTableNames.retainAll(schemaTableNames);
                filteredTableNames.addAll(
                        tableNames.stream().filter(e -> {
                            return systemSchemas.contains(e.getSchemaName());
                        }).collect(Collectors.toList()));
                log.info(
                        "Filter tables by domain <%s + public> in catalog <%s>, original tables: %s, filtered tables: %s",
                        domain, catalogName, tableNames, filteredTableNames);
                return filteredTableNames;
            }
        }
        List<SchemaTableName> schemaTableNames = publicTables.stream().map(e -> {
            return new SchemaTableName(e.getNamespace(), e.getTableName());
        }).collect(Collectors.toList());
        filteredTableNames.retainAll(schemaTableNames);
        filteredTableNames.addAll(
                tableNames.stream().filter(e -> {
                    return systemSchemas.contains(e.getSchemaName());
                }).collect(Collectors.toList()));
        log.info("Filter tables by domain <public> in catalog <%s>, original tables: %s, filtered tables: %s",
                catalogName, tableNames, filteredTableNames);
        return filteredTableNames;
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, AccessControlContext context, CatalogSchemaName schema) {
        List<String> publicAndSystemNamespaces = new ArrayList<>(dbManager.listNamespacesByDomain(publicDomain));
        publicAndSystemNamespaces.addAll(systemSchemas);
        if (getPrincipal(identity).isPresent()) {
            LakeSoulAuthenticatedPrincipal lakesoulPrincipal = getPrincipal(identity).get();
            if (!lakesoulPrincipal.isExpired()) {
                String domain = lakesoulPrincipal.getGroup();
                List<String> namespaces = new ArrayList<>(publicAndSystemNamespaces);
                if (!domain.equals(publicDomain)) {
                    namespaces.addAll(dbManager.listNamespacesByDomain(domain));
                }
                if (!namespaces.contains(schema.getSchemaName())) {
                    throw new AccessDeniedException(
                            String.format(
                                    "Access denied: user '%s' and domain '%s' is not allowed to show tables metadata in schema '%s'.",
                                    lakesoulPrincipal.getSub(), domain, schema.getSchemaName()));
                }
                return;
            }
        }
        if (!publicAndSystemNamespaces.contains(schema.getSchemaName())) {
            throw new AccessDeniedException(
                    String.format("Access denied: user '%s' is not allowed to show tables metadata in schema '%s'.",
                            identity.getUser(), schema.getSchemaName()));
        }
    }

    @Override
    public void checkCanCreateTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table) {
        // no-op
    }

    @Override
    public void checkCanSetTableProperties(Identity identity, AccessControlContext context,
            CatalogSchemaTableName table) {
        // no-op
    }

    @Override
    public void checkCanDropTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table) {
        // no-op
    }

    @Override
    public void checkCanRenameTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table,
            CatalogSchemaTableName newTable) {
        // no-op
    }

    @Override
    public void checkCanAddColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table) {
        // no-op
    }

    @Override
    public void checkCanDropColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table) {
        // no-op
    }

    @Override
    public void checkCanRenameColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table) {
        // no-op
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table,
            Set<String> columns) {
        // no-op
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table) {
        // no-op
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table) {
        // no-op
    }

    @Override
    public void checkCanTruncateTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table) {
        // no-op
    }

    @Override
    public void checkCanUpdateTableColumns(Identity identity, AccessControlContext context,
            CatalogSchemaTableName table, Set<String> updatedColumnNames) {
        // no-op
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, AccessControlContext context, Privilege privilege,
            CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption) {
        // no-op
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, AccessControlContext context, Privilege privilege,
            CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor) {
        // no-op
    }

    @Override
    public void checkCanDropConstraint(Identity identity, AccessControlContext context, CatalogSchemaTableName table) {
        // no-op
    }

    @Override
    public void checkCanAddConstraint(Identity identity, AccessControlContext context, CatalogSchemaTableName table) {
        // no-op
    }

    @Override
    public void checkCanCreateView(Identity identity, AccessControlContext context, CatalogSchemaTableName view) {
        // no-op
    }

    @Override
    public void checkCanRenameView(Identity identity, AccessControlContext context, CatalogSchemaTableName view,
            CatalogSchemaTableName newView) {
        // no-op
    }

    @Override
    public void checkCanDropView(Identity identity, AccessControlContext context, CatalogSchemaTableName view) {
        // no-op
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, AccessControlContext context,
            CatalogSchemaTableName table, Set<String> columns) {
        // no-op
    }

    @Override
    public void checkQueryIntegrity(Identity identity, AccessControlContext context, String query) {
        // no-op
    }
}
