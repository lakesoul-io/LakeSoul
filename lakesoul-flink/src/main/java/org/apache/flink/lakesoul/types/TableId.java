/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.types;

import java.io.Serializable;

public final class TableId implements Serializable {

    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String id;

    /**
     * Create a new table identifier.
     *
     * @param catalogName the name of the database catalog that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param schemaName the name of the database schema that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param tableName the name of the table; may not be null
     */
    public TableId(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        assert this.tableName != null;
        this.id = tableId(this.catalogName, this.schemaName, this.tableName);
    }

    public TableId(io.debezium.relational.TableId dbzTableId) {
        this(dbzTableId.catalog(), dbzTableId.schema(), dbzTableId.table());
    }

    /**
     * Get the name of the JDBC catalog.
     *
     * @return the catalog name, or null if the table does not belong to a catalog
     */
    public String catalog() {
        return catalogName;
    }

    /**
     * Get the name of the JDBC schema.
     *
     * @return the JDBC schema name, or null if the table does not belong to a JDBC schema
     */
    public String schema() {
        return schemaName;
    }

    /**
     * Get the name of the table.
     *
     * @return the table name; never null
     */
    public String table() {
        return tableName;
    }

    public String identifier() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public int compareTo(TableId that) {
        if (this == that) {
            return 0;
        }
        return this.id.compareTo(that.id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableId) {
            return this.compareTo((TableId) obj) == 0;
        }
        return false;
    }

    @Override
    public String toString() {
        return identifier();
    }

    /**
     * Returns a dot-separated String representation of this identifier, quoting all
     * name parts with the {@code "} char.
     */
    public String toDoubleQuotedString() {
        return toQuotedString('"');
    }

    /**
     * Returns a new {@link TableId} with all parts of the identifier using {@code "} character.
     */
    public TableId toDoubleQuoted() {
        return toQuoted('"');
    }

    /**
     * Returns a new {@link TableId} that has all parts of the identifier quoted.
     *
     * @param quotingChar the character to be used to quote the identifier parts.
     */
    public TableId toQuoted(char quotingChar) {
        String catalogName = null;
        if (this.catalogName != null && !this.catalogName.isEmpty()) {
            catalogName = quote(this.catalogName, quotingChar);
        }

        String schemaName = null;
        if (this.schemaName != null && !this.schemaName.isEmpty()) {
            schemaName = quote(this.schemaName, quotingChar);
        }

        return new TableId(catalogName, schemaName, quote(this.tableName, quotingChar));
    }

    /**
     * Returns a dot-separated String representation of this identifier, quoting all
     * name parts with the given quoting char.
     */
    public String toQuotedString(char quotingChar) {
        StringBuilder quoted = new StringBuilder();

        if (catalogName != null && !catalogName.isEmpty()) {
            quoted.append(quote(catalogName, quotingChar)).append(".");
        }

        if (schemaName != null && !schemaName.isEmpty()) {
            quoted.append(quote(schemaName, quotingChar)).append(".");
        }

        quoted.append(quote(tableName, quotingChar));

        return quoted.toString();
    }

    private static String tableId(String catalog, String schema, String table) {
        if (catalog == null || catalog.length() == 0) {
            if (schema == null || schema.length() == 0) {
                return table;
            }
            return schema + "." + table;
        }
        if (schema == null || schema.length() == 0) {
            return catalog + "." + table;
        }
        return catalog + "." + schema + "." + table;
    }

    /**
     * Quotes the given identifier part, e.g. schema or table name.
     */
    private static String quote(String identifierPart, char quotingChar) {
        if (identifierPart == null) {
            return null;
        }

        if (identifierPart.isEmpty()) {
            return new StringBuilder().append(quotingChar).append(quotingChar).toString();
        }

        if (identifierPart.charAt(0) != quotingChar && identifierPart.charAt(identifierPart.length() - 1) != quotingChar) {
            identifierPart = identifierPart.replace(quotingChar + "", repeat(quotingChar));
            identifierPart = quotingChar + identifierPart + quotingChar;
        }

        return identifierPart;
    }

    private static String repeat(char quotingChar) {
        return new StringBuilder().append(quotingChar).append(quotingChar).toString();
    }

    public TableId toLowercase() {
        return new TableId(catalogName, schemaName, tableName.toLowerCase());
    }
}
