# lakesoul-vector: LakeSoul Vector Index Module

This crate provides IVF+RaBitQ approximate nearest neighbor search for LakeSoul,
including index building, incremental insertion, and ANN retrieval on object store.

The IVF+RaBitQ core is vendored from [rabitq-rs](https://github.com/lqhl/rabitq-rs)
into `src/rabitq/`.

## Module Structure

The code is split across two crates:

- **`lakesoul-vector`** — vendored rabitq-rs IVF+RaBitQ core + config types (pure computation, no LakeSoul IO dependency)
- **`lakesoul-io`** — builder, vector extraction, vector search (depends on `lakesoul-vector`, can call `LakeSoulReader`)

```
lakesoul-vector/src/
└── rabitq/                  # Vendored rabitq-rs IVF+RaBitQ core
    ├── mod.rs               # Metric, RabitqError, type exports
    ├── ivf/
    │   ├── mod.rs           # IvfRabitqIndex — search, insert, persistence
    │   ├── builder.rs       # IvfRabitqBuilder — two-pass streaming build
    │   ├── cluster.rs       # ClusterData — unified memory layout
    │   └── lut.rs           # FastScan LUT
    ├── manifest.rs          # ManifestStore — object store persistence (manifest + segment)
    ├── quantizer.rs         # RaBitQ quantization algorithm
    ├── rotation.rs          # FHT random rotation
    ├── simd.rs              # AVX2 FastScan batch distance computation
    ├── kmeans.rs            # K-Means clustering (faer GEMM)
    ├── math.rs              # L2 distance, inner product
    ├── memory.rs            # Aligned memory allocation
    ├── fastscan.rs          # FastScan batch processing
    └── fastscan_kernel.rs   # FastScan + ex-code refinement kernel
└── config.rs                # VectorIndexConfig — index parameters

lakesoul-io/src/
├── vector/
│   ├── builder.rs           # VectorShardIndexBuilder — per-shard index building
│   └── reader.rs            # extract_vector_batch — extract vectors from RecordBatch
├── vector_search.rs         # Vector search: load index → search → return similar IDs
└── reader.rs                # LakeSoulReader::start() calls vector search, injects filter
```

## IVF+RaBitQ Design

### Overview

IVF+RaBitQ combines **Inverted File (IVF)** clustering with **RaBitQ quantization**.
Vectors are partitioned into clusters via K-Means; each cluster is independently
quantized with RaBitQ for compact storage and SIMD-accelerated batch distance computation.

```
┌──────────────────────────────────────────────────────────────────┐
│                     IVF + RaBitQ Index                           │
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐    │
│  │ Cluster 0 │  │ Cluster 1 │  │   ...    │  │ Cluster N-1  │    │
│  │           │  │           │  │          │  │              │    │
│  │ centroid  │  │ centroid  │  │          │  │ centroid     │    │
│  │   +       │  │   +       │  │          │  │   +          │    │
│  │ RaBitQ    │  │ RaBitQ    │  │          │  │ RaBitQ       │    │
│  │ codes     │  │ codes     │  │          │  │ codes        │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

### RaBitQ Quantization

RaBitQ (Rotated Adaptive Binary Ternary Quantization) encodes each vector as:

1. **Binary code** (1 bit/dim): sign of each dimension after rotation
2. **Extended code** (ex_bits/dim): refines the residual for higher accuracy
3. **Reconstruction parameters**: δ, vl, f_add, f_rescale, f_error, f_add_ex, f_rescale_ex

```
Quantization:
  v_rotated = rotate(v)                        // orthogonal random rotation (FHT)
  binary_code[d] = sign(v_rotated[d])          // 1 bit per dimension
  residual = v_rotated - binary_code × delta + vl
  ex_code = quantize(residual, ex_bits)        // multi-bit refinement
```

The **FHT (Fast Hadamard Transform)** rotation evenly distributes variance across
dimensions, making 1-bit quantization more effective. The `faster_config` mode uses
a precomputed scaling factor for 100-500× faster quantization with <1% accuracy loss.

### IVF Index Training

1. **Reservoir sampling**: stream the full dataset once, reservoir-sample `nlist × 64`
   vectors for K-Means training
2. **K-Means**: Lloyd's algorithm with `faer::matmul` GEMM, 15 iterations, `max_points_per_centroid=64`
3. **Streaming rotation + quantization**: stream the dataset a second time, rotate
   each batch in parallel, assign to nearest centroid via GEMM, and quantize with RaBitQ
4. **Cluster construction**: pack quantized vectors into FastScan batch layout
   (32 vectors per batch) in contiguous memory

### Search: FastScan SIMD Batch Distance Computation

1. **Cluster selection**: rotate query → L2 distance to all centroids → partial sort → top `nprobe`
2. **Batch search**: for each selected cluster, process in batches of 32:
   - Build LUT (4 dimensions → 16 entries, quantized to i8)
   - SIMD accumulate (AVX2)
   - Estimate distance: `est_dist = f_add + g_add + f_rescale × binary_term`
   - Lower-bound pruning: skip vectors that cannot make top-K
   - Ex-code refinement (survivors): unpack ex-code on demand, compute exact distance
   - Update distance heap (top-K)

High-accuracy mode (`padded_dim > 2048`): uses 16-bit split LUT to prevent u16 accumulator overflow.

### Memory Layout (ClusterData)

```
ClusterData (per cluster)
├── centroid: Vec<f32>                         // padded_dim × 4 B
├── ids: Vec<usize>                            // n × 8 B
├── batch_data: Vec<u8>                        // contiguous FastScan layout
│   └── [Batch 0][Batch 1]...[Batch N]
│       └── per batch (stride = pd×4 + 32×12 B):
│           ├── packed_binary_codes: pd × 32 / 8 B
│           ├── f_add:    32 × 4 B
│           ├── f_rescale: 32 × 4 B
│           └── f_error:  32 × 4 B
├── ex_codes_packed: Vec<Vec<u8>>              // per-vector packed ex codes
├── f_add_ex, f_rescale_ex: Vec<f32>           // per-vector ex params
├── delta, vl: Vec<f32>                        // reconstruction params
├── pending_ids, pending_vectors               // insert buffer (≤31 vectors)
└── num_vectors, padded_dim, ex_bits           // metadata
```

## LakeSoul Integration

### Design Principles

1. **Write path unchanged**: data writing and index building are fully decoupled.
   Writes proceed normally (parquet → flush → commit to PG)
2. **Compaction-driven**: indexes are built by background compaction tasks,
   following the `newCompaction` pattern
3. **Per-shard parallelism**: each shard (partition_desc + hash_bucket_id) builds
   its index independently. Indexes are stored at `{table_path}/_vector_index/{col}/{partition}/{bucket}/`

### Index Building Flow

```
┌───────────────────────────────────────────────────────┐
│ VectorShardIndexBuilder (Rust)                        │
│                                                       │
│ build():                                              │
│   ├─ manifest_exists()? → Fresh or Loaded             │
│   │                                                   │
│   ├─ Fresh (full build):                              │
│   │   ├─ IvfRabitqBuilder::new() — offline mode       │
│   │   ├─ Pass 1: stream parquet → reservoir sample    │
│   │   ├─ Pass 2: builder.build(make_stream)           │
│   │   │   └─ re-create stream → K-Means + quantize    │
│   │   └─ index.save_to_v4(&mstore) — write manifest   │
│   │                                                   │
│   └─ Loaded (incremental):                            │
│       ├─ IvfRabitqBuilder::load(&mstore) — load centroids
│       ├─ stream → insert_batch (quantize + append)    │
│       └─ builder.flush(&mstore) — write delta segments│
└───────────────────────────────────────────────────────┘
```

### Python Call Chain

```
Python build_partition_vector_index()
  ├─ 1. PG: select_table_info → PK column, table_path
  ├─ 2. PG: get_partition_info → latest version, snapshot UUIDs
  ├─ 3. PG: list_data_commit_info → DataFileOp (file paths)
  ├─ 4. Group files by bucket_id
  └─ 5. build_shard_vector_index() × N (PyO3 → Rust)
       └─ VectorShardIndexBuilder::build()
```

### Search Flow

Vector search is integrated into `LakeSoulReader::start()`. When `LakeSoulIOConfig`
options contain `vector_search_*` parameters, the reader automatically:

```
LakeSoulReader::start()
  ├─ get_filter_exprs() → user-provided filters
  ├─ inject_vector_search_filter()
  │   ├─ Get ObjectStore from session RuntimeEnv
  │   ├─ search_matching_shards() → per-shard index lookup
  │   │   └─ ManifestStore(store, index_prefix) → IvfRabitqIndex::load_from_v4() → search()
  │   └─ IDs → pk IN (id1, id2, ...) DataFusion Expr filter
  └─ build_physical_plan(filters) → standard read path + vector search filter
```

### Object Storage Persistence (V4)

rabitq-rs uses immutable files, suitable for object storage (S3/GCS/Azure):

```
{table_path}/_vector_index/{col}/{partition}/{bucket}/
├── LATEST                     # Latest manifest version pointer (CAS)
├── manifests/
│   ├── g00000001_v00000001.bin
│   └── g00000001_v00000002.bin
├── cluster_0000_v0000.seg     # Base segment (initial build)
├── cluster_0000_v0001.seg     # Delta segment (1st incremental)
└── cluster_0000_v0002.seg     # Delta segment (2nd incremental)
```

**Delta Segment Design**: incremental flushes write delta segments containing only
new vectors. Existing files are never read, modified, or deleted. Manifest uses
CAS (Compare-And-Swap) for concurrency safety.

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `dim` | Vector dimension | required |
| `nlist` | IVF cluster count | 256 |
| `total_bits` | RaBitQ total bits (1-16) | 7 |
| `metric` | Distance metric | L2 |
| `rotator_type` | Rotator type | FhtKacRotator |
| `seed` | Random seed | 42 |
| `use_faster_config` | Fast quantization mode | true |

## Performance Reference

Based on rabitq-rs benchmark results on GIST-1M (960D, 1M vectors, 4096 clusters):

| Metric | Value |
|--------|-------|
| Full build (7-bit) | 74s, peak RSS 1.88 GB |
| Index memory | 952 MB |
| Search (nprobe=64, topk=10, Recall@100=83.0%) | ~1,100 QPS |
| Search (nprobe=128, Recall@100=92.2%) | ~845 QPS |
| Incremental insert throughput | ~7,800 vec/s |
