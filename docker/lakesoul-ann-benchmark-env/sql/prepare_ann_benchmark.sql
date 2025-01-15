USE lakesoul;
CREATE NAMESPACE IF NOT EXISTS ann_benchmark;

-- 创建向量表
CREATE TABLE IF NOT EXISTS ann_benchmark.embeddings (
  id BIGINT NOT NULL,
  embedding ARRAY<FLOAT>, 
  label STRING,
  PRIMARY KEY(id)
) USING lakesoul
LOCATION 's3://lakesoul-test-bucket/data/ann_benchmark/embeddings'
TBLPROPERTIES (
  'hashBucketNum' = '4',
  'hashPartitions' = 'id'
);

-- 使用 LocalSensitiveHashANN 加载数据
INSERT OVERWRITE lakesoul.ann_benchmark.embeddings
SELECT * FROM LocalSensitiveHashANN.loadEmbeddings('/data/embeddings/vectors.parquet'); 