// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.metadata;

import org.apache.flink.table.catalog.CatalogDatabase;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LakesoulCatalogDatabase implements CatalogDatabase {

    private static final String DefaultComment = "Default Comment";
    private final Map<String, String> properties;
    private final String comment;


    public LakesoulCatalogDatabase(Map<String, String> properties, String comment) {
        this.properties = new HashMap<>();
        if (properties != null) {
            this.properties.putAll(properties);
        }
        this.comment = comment;
    }

    public LakesoulCatalogDatabase() {
        this(null, DefaultComment);
    }


    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public CatalogDatabase copy() {
        return new LakesoulCatalogDatabase();
    }

    @Override
    public CatalogDatabase copy(Map<String, String> map) {
        return new LakesoulCatalogDatabase();
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.ofNullable(comment);
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.ofNullable(comment);
    }
}
