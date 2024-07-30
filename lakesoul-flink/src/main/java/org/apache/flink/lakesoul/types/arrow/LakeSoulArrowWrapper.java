// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types.arrow;

import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.table.runtime.arrow.ArrowUtils;

import java.io.*;
import java.nio.channels.Channels;
import java.util.Base64;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class LakeSoulArrowWrapper implements Serializable {
    private final byte[] encodedBatch;

    private final byte[] encodedTableInfo;

    public LakeSoulArrowWrapper(TableInfo tableInfo, VectorSchemaRoot vectorSchemaRoot) {
        this.encodedBatch = encodeBatch(vectorSchemaRoot);
        this.encodedTableInfo = tableInfo.toByteArray();
    }

    public LakeSoulArrowWrapper(byte[] encodedTableInfo, VectorSchemaRoot vectorSchemaRoot) {
        this.encodedBatch = encodeBatch(vectorSchemaRoot);
        this.encodedTableInfo = encodedTableInfo;
    }

    public LakeSoulArrowWrapper(byte[] encodedTableInfo, byte[] encodedBatch) {
        this.encodedTableInfo = encodedTableInfo;
        this.encodedBatch = encodedBatch;
    }

    @Override
    public String toString() {
        AtomicReference<String> result = new AtomicReference<>();
        withDecoded(ArrowUtils.getRootAllocator(), (tableInfo, recordBatch) -> {
            result.set("LakeSoulVectorSchemaRootWrapper{" +
                    "tableInfo=" + tableInfo +
                    ", vectorSchemaRoot=" + recordBatch.contentToTSVString() +
                    '}');

        });
        return result.get();
    }

    public void withDecoded(BufferAllocator allocator, BiConsumer<TableInfo, VectorSchemaRoot> consumer) {
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(encodedBatch), allocator)) {
            reader.loadNextBatch();
            TableInfo tableInfo = TableInfo.parseFrom(encodedTableInfo);
            consumer.accept(tableInfo, reader.getVectorSchemaRoot());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] encodeBatch(VectorSchemaRoot vectorSchemaRoot) {
        try (
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, /*DictionaryProvider=*/null,
                        Channels.newChannel(out));
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
            return out.toByteArray();
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

    public TableSchemaIdentity generateTableSchemaIdentity() {
        try {
            TableInfo tableInfo = TableInfo.parseFrom(encodedTableInfo);
            return TableSchemaIdentity.fromTableInfo(tableInfo);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        TableInfo info = TableInfo.parseFrom(Base64.getDecoder().decode("CghOT1RfVVNFRBIEdGVzdBoSc19xYXJhX2ZsaWdodF9pbmZvIkNoZGZzOi8vMTAuNzkuMTEuMjg6OTAwMC90bXAvZmxpbmsvd2FyZWhvdXNlL3Rlc3Qvc19xYXJhX2ZsaWdodF9pbmZvKp8cewogICJmaWVsZHMiIDogWyB7CiAgICAibmFtZSIgOiAiaGRmX2ZpbGVfbmFtZSIsCiAgICAibnVsbGFibGUiIDogdHJ1ZSwKICAgICJ0eXBlIiA6IHsKICAgICAgIm5hbWUiIDogInV0ZjgiCiAgICB9LAogICAgImNoaWxkcmVuIiA6IFsgXQogIH0sIHsKICAgICJuYW1lIiA6ICJmaWxlX25vIiwKICAgICJudWxsYWJsZSIgOiB0cnVlLAogICAgInR5cGUiIDogewogICAgICAibmFtZSIgOiAidXRmOCIKICAgIH0sCiAgICAiY2hpbGRyZW4iIDogWyBdCiAgfSwgewogICAgIm5hbWUiIDogImRiX3ZlcnNpb24iLAogICAgIm51bGxhYmxlIiA6IHRydWUsCiAgICAidHlwZSIgOiB7CiAgICAgICJuYW1lIiA6ICJ1dGY4IgogICAgfSwKICAgICJjaGlsZHJlbiIgOiBbIF0KICB9LCB7CiAgICAibmFtZSIgOiAiZGJfbGV2ZWwiLAogICAgIm51bGxhYmxlIiA6IHRydWUsCiAgICAidHlwZSIgOiB7CiAgICAgICJuYW1lIiA6ICJ1dGY4IgogICAgfSwKICAgICJjaGlsZHJlbiIgOiBbIF0KICB9LCB7CiAgICAibmFtZSIgOiAiYWNtc2ZsaWdodHBoYXNlbm8iLAogICAgIm51bGxhYmxlIiA6IHRydWUsCiAgICAidHlwZSIgOiB7CiAgICAgICJuYW1lIiA6ICJ1dGY4IgogICAgfSwKICAgICJjaGlsZHJlbiIgOiBbIF0KICB9LCB7CiAgICAibmFtZSIgOiAiY2hhbm5lbG5vIiwKICAgICJudWxsYWJsZSIgOiB0cnVlLAogICAgInR5cGUiIDogewogICAgICAibmFtZSIgOiAidXRmOCIKICAgIH0sCiAgICAiY2hpbGRyZW4iIDogWyBdCiAgfSwgewogICAgIm5hbWUiIDogImZpbGVuYW1lIiwKICAgICJudWxsYWJsZSIgOiB0cnVlLAogICAgInR5cGUiIDogewogICAgICAibmFtZSIgOiAidXRmOCIKICAgIH0sCiAgICAiY2hpbGRyZW4iIDogWyBdCiAgfSwgewogICAgIm5hbWUiIDogInRyaWdnZXJjb2RlIiwKICAgICJudWxsYWJsZSIgOiB0cnVlLAogICAgInR5cGUiIDogewogICAgICAibmFtZSIgOiAidXRmOCIKICAgIH0sCiAgICAiY2hpbGRyZW4iIDogWyBdCiAgfSwgewogICAgIm5hbWUiIDogInRhaWwiLAogICAgIm51bGxhYmxlIiA6IHRydWUsCiAgICAidHlwZSIgOiB7CiAgICAgICJuYW1lIiA6ICJ1dGY4IgogICAgfSwKICAgICJjaGlsZHJlbiIgOiBbIF0KICB9LCB7CiAgICAibmFtZSIgOiAiYWN0eXBlIiwKICAgICJudWxsYWJsZSIgOiB0cnVlLAogICAgInR5cGUiIDogewogICAgICAibmFtZSIgOiAidXRmOCIKICAgIH0sCiAgICAiY2hpbGRyZW4iIDogWyBdCiAgfSwgewogICAgIm5hbWUiIDogImRlY29kZXJ2ZXJzaW9uIiwKICAgICJudWxsYWJsZSIgOiB0cnVlLAogICAgInR5cGUiIDogewogICAgICAibmFtZSIgOiAidXRmOCIKICAgIH0sCiAgICAiY2hpbGRyZW4iIDogWyBdCiAgfSwgewogICAgIm5hbWUiIDogIm91dHB1dGxheW91dCIsCiAgICAibnVsbGFibGUiIDogdHJ1ZSwKICAgICJ0eXBlIiA6IHsKICAgICAgIm5hbWUiIDogInV0ZjgiCiAgICB9LAogICAgImNoaWxkcmVuIiA6IFsgXQogIH0sIHsKICAgICJuYW1lIiA6ICJhcnJpdmFsYWlycG9ydCIsCiAgICAibnVsbGFibGUiIDogdHJ1ZSwKICAgICJ0eXBlIiA6IHsKICAgICAgIm5hbWUiIDogInV0ZjgiCiAgICB9LAogICAgImNoaWxkcmVuIiA6IFsgXQogIH0sIHsKICAgICJuYW1lIiA6ICJhcnJpdmFscnVud2F5IiwKICAgICJudWxsYWJsZSIgOiB0cnVlLAogICAgInR5cGUiIDogewogICAgICAibmFtZSIgOiAidXRmOCIKICAgIH0sCiAgICAiY2hpbGRyZW4iIDogWyBdCiAgfSwgewogICAgIm5hbWUiIDogImRlcGFydHVyZWFpcnBvcnQiLAogICAgIm51bGxhYmxlIiA6IHRydWUsCiAgICAidHlwZSIgOiB7CiAgICAgICJuYW1lIiA6ICJ1dGY4IgogICAgfSwKICAgICJjaGlsZHJlbiIgOiBbIF0KICB9LCB7CiAgICAibmFtZSIgOiAiZGVwYXJ0dXJlcnVud2F5IiwKICAgICJudWxsYWJsZSIgOiB0cnVlLAogICAgInR5cGUiIDogewogICAgICAibmFtZSIgOiAidXRmOCIKICAgIH0sCiAgICAiY2hpbGRyZW4iIDogWyBdCiAgfSwgewogICAgIm5hbWUiIDogImFycml2YWxkYXRldGltZSIsCiAgICAibnVsbGFibGUiIDogdHJ1ZSwKICAgICJ0eXBlIiA6IHsKICAgICAgIm5hbWUiIDogInV0ZjgiCiAgICB9LAogICAgImNoaWxkcmVuIiA6IFsgXQogIH0sIHsKICAgICJuYW1lIiA6ICJkZXBhcnR1cmVkYXRldGltZSIsCiAgICAibnVsbGFibGUiIDogdHJ1ZSwKICAgICJ0eXBlIiA6IHsKICAgICAgIm5hbWUiIDogInV0ZjgiCiAgICB9LAogICAgImNoaWxkcmVuIiA6IFsgXQogIH0sIHsKICAgICJuYW1lIiA6ICJsYW5kaW5nZGF0ZXRpbWUiLAogICAgIm51bGxhYmxlIiA6IHRydWUsCiAgICAidHlwZSIgOiB7CiAgICAgICJuYW1lIiA6ICJ1dGY4IgogICAgfSwKICAgICJjaGlsZHJlbiIgOiBbIF0KICB9LCB7CiAgICAibmFtZSIgOiAidGFrZW9mZmRhdGV0aW1lIiwKICAgICJudWxsYWJsZSIgOiB0cnVlLAogICAgInR5cGUiIDogewogICAgICAibmFtZSIgOiAidXRmOCIKICAgIH0sCiAgICAiY2hpbGRyZW4iIDogWyBdCiAgfSwgewogICAgIm5hbWUiIDogImZsaWdodG51bWJlciIsCiAgICAibnVsbGFibGUiIDogdHJ1ZSwKICAgICJ0eXBlIiA6IHsKICAgICAgIm5hbWUiIDogInV0ZjgiCiAgICB9LAogICAgImNoaWxkcmVuIiA6IFsgXQogIH0sIHsKICAgICJuYW1lIiA6ICJmbGlnaHR0eXBlIiwKICAgICJudWxsYWJsZSIgOiB0cnVlLAogICAgInR5cGUiIDogewogICAgICAibmFtZSIgOiAidXRmOCIKICAgIH0sCiAgICAiY2hpbGRyZW4iIDogWyBdCiAgfSwgewogICAgIm5hbWUiIDogImRhdGF2ZXJzaW9uIiwKICAgICJudWxsYWJsZSIgOiB0cnVlLAogICAgInR5cGUiIDogewogICAgICAibmFtZSIgOiAidXRmOCIKICAgIH0sCiAgICAiY2hpbGRyZW4iIDogWyBdCiAgfSwgewogICAgIm5hbWUiIDogImluc3RhbGxhdGlvbmRhdGUiLAogICAgIm51bGxhYmxlIiA6IHRydWUsCiAgICAidHlwZSIgOiB7CiAgICAgICJuYW1lIiA6ICJ1dGY4IgogICAgfSwKICAgICJjaGlsZHJlbiIgOiBbIF0KICB9LCB7CiAgICAibmFtZSIgOiAicmVtb3ZhbGRhdGUiLAogICAgIm51bGxhYmxlIiA6IHRydWUsCiAgICAidHlwZSIgOiB7CiAgICAgICJuYW1lIiA6ICJ1dGY4IgogICAgfSwKICAgICJjaGlsZHJlbiIgOiBbIF0KICB9LCB7CiAgICAibmFtZSIgOiAicmVjb3JkZXJ0eXBlaWQiLAogICAgIm51bGxhYmxlIiA6IHRydWUsCiAgICAidHlwZSIgOiB7CiAgICAgICJuYW1lIiA6ICJ1dGY4IgogICAgfSwKICAgICJjaGlsZHJlbiIgOiBbIF0KICB9LCB7CiAgICAibmFtZSIgOiAiYWlyYm9ybmVkdXJhdGlvbiIsCiAgICAibnVsbGFibGUiIDogdHJ1ZSwKICAgICJ0eXBlIiA6IHsKICAgICAgIm5hbWUiIDogInV0ZjgiCiAgICB9LAogICAgImNoaWxkcmVuIiA6IFsgXQogIH0sIHsKICAgICJuYW1lIiA6ICJmbGlnaHRkdXJhdGlvbiIsCiAgICAibnVsbGFibGUiIDogdHJ1ZSwKICAgICJ0eXBlIiA6IHsKICAgICAgIm5hbWUiIDogInV0ZjgiCiAgICB9LAogICAgImNoaWxkcmVuIiA6IFsgXQogIH0sIHsKICAgICJuYW1lIiA6ICJmbHRkdCIsCiAgICAibnVsbGFibGUiIDogdHJ1ZSwKICAgICJ0eXBlIiA6IHsKICAgICAgIm5hbWUiIDogInV0ZjgiCiAgICB9LAogICAgImNoaWxkcmVuIiA6IFsgXQogIH0gXQp9OgJ7fUINZmx0ZHQ7ZmlsZV9ubw=="));
        System.out.println(info);

        try (BufferedReader br = new BufferedReader(new FileReader("/Users/ceng/Desktop/123.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                // process the line.
                System.out.println(line.substring(0, 20));
                String base64 = line.substring(11).trim();
//                System.out.println(base64);
                byte[] encodedBatch = Base64.getDecoder().decode(base64);
                BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(encodedBatch), allocator)) {
                    reader.loadNextBatch();
                    VectorSchemaRoot batch = reader.getVectorSchemaRoot();
                    String content = batch.contentToTSVString();
//                    System.out.println(content);
                } catch (IOException e) {
                    System.out.println("LakeSoulArrowWrapper::withDecoded error: " + e);
                    throw new RuntimeException(e);
                }
                break;
            }
        }

    }
}
