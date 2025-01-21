USE lakesoul;
CREATE NAMESPACE IF NOT EXISTS ann_benchmark;

-- 从 HDF5 文件创建临时视图
CREATE OR REPLACE TEMPORARY VIEW Dataset AS
SELECT * FROM LocalSensitiveHashANN.loadHDF5('/data/embeddings/fashion-mnist-784-euclidean.hdf5');

-- 创建向量表，包含原始向量和 LSH 向量
CREATE TABLE IF NOT EXISTS ann_benchmark.embeddings (
  id BIGINT NOT NULL,
  embedding ARRAY<FLOAT>,
  embedding_LSH ARRAY<FLOAT>,
  PRIMARY KEY(id)
) USING lakesoul
LOCATION 's3://lakesoul-test-bucket/data/ann_benchmark/embeddings'
TBLPROPERTIES (
  'hashBucketNum' = '4',
  'hashPartitions' = 'id'
);

-- 从临时视图加载数据并计算 LSH
INSERT OVERWRITE lakesoul.ann_benchmark.embeddings
SELECT 
  id,
  embedding,
  LocalSensitiveHashANN.computeLSH(embedding) as embedding_LSH
FROM Dataset; 