package org.apache.flink.lakesoul.types.arrow;

import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.table.runtime.arrow.ArrowUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class LakeSoulArrowWrapper implements Serializable {
    private final byte[] encodedBatch;

    private final byte[] encodedTableInfo;

    public LakeSoulArrowWrapper(TableInfo tableInfo, VectorSchemaRoot vectorSchemaRoot) {
        try (
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, /*DictionaryProvider=*/null, Channels.newChannel(out));
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
            this.encodedBatch = out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.encodedTableInfo = tableInfo.toByteArray();
    }


    public LakeSoulArrowWrapper(byte[] encodedTableInfo, byte[] encodedBatch) {
        this.encodedTableInfo = encodedTableInfo;
        this.encodedBatch = encodedBatch;
    }


    @Override
    public String toString() {
        AtomicReference<String> result = new AtomicReference<>();
        withDecoded((recordBatch) -> {
            try {
                result.set("LakeSoulVectorSchemaRootWrapper{" +
                        "tableInfo=" + TableInfo.parseFrom(encodedTableInfo) +
                        ", vectorSchemaRoot=" + recordBatch.contentToTSVString() +
                        '}');
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        });
        return result.get();
    }

    public void withDecoded(Consumer<VectorSchemaRoot> consumer) {
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(encodedBatch), ArrowUtils.getRootAllocator())) {
            reader.loadNextBatch();
            VectorSchemaRoot recordBatch = reader.getVectorSchemaRoot();
            consumer.accept(recordBatch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public byte[] getEncodedBatch() {
        return encodedBatch;
    }

    public byte[] getEncodedTableInfo() {
        return encodedTableInfo;
    }
}
