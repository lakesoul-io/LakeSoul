package test.org.apache.arrow;


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
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.StreamSupport;



/**
 * Test cases are from https://arrow.apache.org/cookbook/java/dataset.html
 */
public class DatasetSuite {

    String uri = "file:///Users/ceng/test/table/table_test1/gender=Female/part-00000-5a483d61-bd4b-4fd3-831b-f5eaea3055f8.c000.snappy.parquet";
    @Test
    public void AutoInferedSchema(){
//        String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
//        String uri = "src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet";

//        String uri = "file:///Users/ceng/Documents/GitHub/LakeSoul/arrow/src/test/resources/sample-parquet-files/test.parquet";
        ScanOptions options = new ScanOptions(/*batchSize*/ 100);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options)
        ) {
            System.out.println(StreamSupport.stream(scanner.scan().spliterator(), false).count());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void PredefinedSchame() {

//        String uri = "file:///Users/ceng/Documents/GitHub/LakeSoul/arrow/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet";
        ScanOptions options = new ScanOptions(/*batchSize*/ 100);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
                Dataset dataset = datasetFactory.finish(datasetFactory.inspect());
                Scanner scanner = dataset.newScan(options)
        ) {
            System.out.println(StreamSupport.stream(scanner.scan().spliterator(), false).count());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void GettingSchemaDuringDatasetConstruction() {
//        String uri = "file:///Users/ceng/Documents/GitHub/LakeSoul/arrow/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet";
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri)
        ) {
            Schema schema = datasetFactory.inspect();
            System.out.println(schema);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void GettingSchemaFromDataset() {
//        String uri = "file:///Users/ceng/Documents/GitHub/LakeSoul/arrow/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet";
        ScanOptions options = new ScanOptions(/*batchSize*/ 1);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options)
        ) {
            Schema schema = scanner.schema();

            System.out.println(schema);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void QueryDataContentWithProjection() {
//        String uri = "file:///Users/ceng/Documents/GitHub/LakeSoul/arrow/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet";
        ScanOptions options = new ScanOptions(/*batchSize*/ 1024);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options)
        ) {
            Schema schema = scanner.schema();

            scanner.scan().forEach(scanTask-> {
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                VectorLoader loader = new VectorLoader(root);
                try (ScanTask.BatchIterator iterator = scanTask.execute()) {
                    int cnt = 0;
                    int batchNum = 0;
                    while (iterator.hasNext()) {
                        try (ArrowRecordBatch batch = iterator.next()) {
                            System.out.println(batch);
                            cnt += batch.getLength();
                            batchNum += 1;
                            System.out.println(batch.getLength());
                            loader.load(batch);
                            System.out.println(root.contentToTSVString());
                        }
                    }
                    System.out.println(batchNum);
                    System.out.println(cnt);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            System.out.println(schema);
            allocator.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

//    @Test
//    public void QueryDataContentForFile() {
//        String uri = "file:///Users/ceng/Documents/GitHub/LakeSoul/arrow/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet";
//        ScanOptions options = new ScanOptions(/*batchSize*/ 100);
//        try (
//                BufferAllocator allocator = new RootAllocator();
//                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
//                Dataset dataset = datasetFactory.finish();
//                Scanner scanner = dataset.newScan(options)
//        ) {
//            scanner.scan().forEach(scanTask -> {
//                try (ArrowReader reader = scanTask.execute()) {
//                    while (reader.loadNextBatch()) {
//                        try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
//                            System.out.print(root.contentToTSVString());
//                        }
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            });
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//
//    @Test
//    public void QueryDataContentForDirectory() {
//        String uri = "file:///Users/ceng/Documents/GitHub/LakeSoul/arrow/src/test/resources/sample-parquet-files/";
//        ScanOptions options = new ScanOptions(/*batchSize*/ 100);
//        try (BufferAllocator allocator = new RootAllocator();
//             DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
//             Dataset dataset = datasetFactory.finish();
//             Scanner scanner = dataset.newScan(options)
//        ) {
//            scanner.scan().forEach(scanTask-> {
//                final int[] count = {1};
//                try (ArrowReader reader = scanTask.execute()) {
//                    while (reader.loadNextBatch()) {
//                        try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
//                            System.out.println("Batch: " + count[0]++ + ", RowCount: " + root.getRowCount());
//                        }
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            });
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }


//    @Test
//    public void QueryDataContentWithProjection() {
//        String uri;
//        uri = "file:///Users/ceng/Desktop/test/table/table_test/gender=Female/part-00000-ca90c62a-fbde-4068-a513-2d8cf1f5c819.c000.snappy.parquet";
////        uri = "file:///Users/ceng/Desktop/test/parquet/base-0.parquet";
//        uri = "file:///Users/ceng/Desktop/test/result/part-00000-0a28a20e-6267-43b0-aef7-564fe0354fed-c000.snappy.parquet";
//        String[] projection = new String[] {"first_name", "last_name"};
//        ScanOptions options = new ScanOptions(/*batchSize*/ 100);
//        try (
//                BufferAllocator allocator = new RootAllocator();
//                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
//                Dataset dataset = datasetFactory.finish();
//                Scanner scanner = dataset.newScan(options)
//        ) {
//            scanner.scan().forEach(scanTask-> {
//                try (ArrowReader reader = scanTask.execute()) {
//                    int cnt = 0;
//                    int batchNum = 0;
//                    while (reader.loadNextBatch()) {
//                        try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
//                            cnt += root.getRowCount();
//                            batchNum += 1;
//                            System.out.print(root.contentToTSVString());
//                        }
//                    }
//                    System.out.println(batchNum);
//                    System.out.println(cnt);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            });
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
}
