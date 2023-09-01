// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.handle;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/** table handle */
public class LakeSoulTableHandle implements ConnectorTableHandle {

    private String id;
    private SchemaTableName names;

    @JsonCreator
    public LakeSoulTableHandle(
            @JsonProperty("id")  String id,
            @JsonProperty("names") SchemaTableName names) {
        this.id = requireNonNull(id, "id should not be null");
        this.names = requireNonNull(names, "names should not be null");
    }

    @JsonProperty
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty
    public SchemaTableName getNames() {
        return names;
    }

    public void setNames(SchemaTableName names) {
        this.names = names;
    }


}