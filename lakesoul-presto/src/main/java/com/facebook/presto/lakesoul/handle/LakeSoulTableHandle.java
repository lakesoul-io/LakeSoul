// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.handle;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;

/** table handle */
public class LakeSoulTableHandle implements ConnectorTableHandle {
    private String id;
    private SchemaTableName names;

    public LakeSoulTableHandle(String id, SchemaTableName names) {
        this.id = id;
        this.names = names;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public SchemaTableName getNames() {
        return names;
    }

    public void setNames(SchemaTableName names) {
        this.names = names;
    }


}