/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DatabaseSchemaedTables {
    private final String DBName;
    HashMap<String, Table> tables = new HashMap<>(20);

    public DatabaseSchemaedTables(String DBName) {
        this.DBName = DBName;
    }

    public Table addTable(String tableName) {
        if (!tables.containsKey(tableName)) {
            Table tb = new Table(tableName);
            tables.put(tableName, tb);
        }
        return tables.get(tableName);
    }

    class Table {
        String TName;
        List<Column> cols = new ArrayList<>(50);
        List<PK> PKs = new ArrayList<>(10);

        public Table(String TName) {
            this.TName = TName;
        }

        public void addColumn(String colName, String colType) {
            cols.add(new Column(colName, colType));
        }

        public void addPrimaryKey(String key, int index) {
            if (key != null) {
                PKs.add(new PK(key, index));
            }
        }
    }

    class Column {
        String colName;
        String type;

        public Column(String colName, String type) {
            this.colName = colName;
            this.type = type;
        }
    }

    class PK implements Comparable<PK> {
        String colName;
        int pos;

        public PK(String colName, int pos) {
            this.colName = colName;
            this.pos = pos;
        }

        @Override
        public int compareTo(PK o) {
            return this.pos - o.pos;
        }
    }
}
