package org.apache.flink.lakesoul.types.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;

import java.io.Serializable;

public class LakeSoulArrowWrapper implements Serializable {
    TableSchemaIdentity tableSchemaIdentity;
    VectorSchemaRoot vectorSchemaRoot;

    String content;

    public LakeSoulArrowWrapper(TableSchemaIdentity tableSchemaIdentity, VectorSchemaRoot vectorSchemaRoot, String content) {
        this.tableSchemaIdentity = tableSchemaIdentity;
        this.vectorSchemaRoot = vectorSchemaRoot;
        this.content = content;
    }


    public LakeSoulArrowWrapper(TableSchemaIdentity tableSchemaIdentity, VectorSchemaRoot vectorSchemaRoot) {
        this(tableSchemaIdentity, vectorSchemaRoot, (vectorSchemaRoot == null ? "null" : vectorSchemaRoot.contentToTSVString()));
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        return vectorSchemaRoot;
    }

    public TableSchemaIdentity getTableSchemaIdentity() {
        return tableSchemaIdentity;
    }

    @Override
    public String toString() {
        return "LakeSoulVectorSchemaRootWrapper{" +
                "tableSchemaIdentity=" + tableSchemaIdentity +
                ", vectorSchemaRoot=" + content +
                '}';
    }
}
