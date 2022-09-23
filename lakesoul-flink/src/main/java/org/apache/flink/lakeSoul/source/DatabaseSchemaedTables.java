package org.apache.flink.lakeSoul.source;

//import com.dmetasoul.lakesoul.meta.FlinkCDCTypeConvert;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DatabaseSchemaedTables {
    private String DBName;
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
//        public RowType toFlinkRowType(){
//            int len = cols.size();
//            LogicalType[] lt = new LogicalType[len];
//            for(Column col : cols){
//                FlinkCDCTypeConvert.convertLogicalType(col.type);
//            }
//            return null;
//        }
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
