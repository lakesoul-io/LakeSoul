/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.lakesoul.io;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowJavaReader {
    private final ScanOptions options;
    private final BufferAllocator allocator;
    private final DatasetFactory datasetFactory;
    private final Dataset dataset;
    private final Scanner scanner;
    private final Schema schema;
    private final ScanTask.BatchIterator batchIterator;

    public static class ArrowJavaReaderBuilder {
        private String uri;
        private long batchSize;
        ArrowJavaReader build() {
            return new ArrowJavaReader(uri, batchSize);
        }

        public void setUri(String uri) {
            this.uri = uri;
        }

        public void setBatchSize(long batchSize) {
            this.batchSize = batchSize;
        }
    }

    ArrowJavaReader(String uri, long batchSize){
        options = new ScanOptions(/*batchSize*/ batchSize);
        allocator = new RootAllocator();
        datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
        dataset = datasetFactory.finish();
        scanner = dataset.newScan(options);
        schema = scanner.schema();
        batchIterator = scanner.scan().iterator().next().execute();
    }

    void nextRecordBatch(long schemaAddr, long arrayAddr, NativeIOWrapper.Callback callback) {
        try {
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            VectorLoader loader = new VectorLoader(root);
            if (batchIterator.hasNext()) {
                try (ArrowRecordBatch batch = batchIterator.next()) {
                    loader.load(batch);
                    Data.exportVectorSchemaRoot(allocator, root, null, ArrowArray.wrap(arrayAddr), ArrowSchema.wrap(schemaAddr));
                    callback.invoke(true,"");
                }
            } else {
                callback.invoke(false,"");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
