# LakeSoul 具身数据处理支持 · 执行计划

- 版本：v1.2（用户功能驱动版，M1 重排）
- 日期：2026-09-18
- 状态：M1 完成；M2 进行中（M2-4 `import_lerobot` v3 基础版已完成；blob 外置待设计评审）
- 修订点（相对 v1.1）：
  - 从"低层 RowSelection / 全局行号"改为从**用户训练闭环**倒推功能；
  - 确认：episode 内"顺序消费 + 窗口随机起点"，不做全局行随机访问；
  - 确认：M1 视频先用 per-frame 图片 bytes（GOP blob 留给 M2）；
  - 确认：`episode_id` 分区（或粗粒度 bucket）+ 时间有序 + 独立 frames/GOP 表为硬约束；
  - RowSelection / 行级 range 下推延后到 M2，与 blob range 一起做。

## 0. 总目标

训练态表默认 `vortex`（非 compact）；blob 外置但对用户透明；对齐/导入做在 Python SDK；自定义 layout 放本仓、可上游；全部直接进 OSS 主线。

## 1. 设计原则（已达成的决策）

| 决策 | 落地含义 |
|---|---|
| 视频+状态优先 | 存储布局、窗口组装、对齐围绕"状态 ticks + 帧/GOP"设计 |
| 只走 Vortex 快路径（非 compact） | 训练态表默认 `file_format=vortex`；冷列用 per-column 策略单独 compact |
| 外置 blob 可接受，但透明封装 | 用户读写的仍是普通 bytes 列；offload、range、GC、缓存全在引擎/SDK 内完成 |
| 对齐/导入做在 Python SDK | `lakesoul.embodied`：选择、窗口、采样、解码、dataloader |
| 允许自研 layout + blob | 自定义 Vortex layout 放本仓、扩展注册，不等待上游 |
| 直接进 OSS 主线 | 公共 API、文档、测试、benchmark、示例一并进仓 |
| episode 顺序消费 + 窗口随机起点 | 不做全局行随机访问；随机性来自 anchor 置换，分布式按 episode/chunk 单元切分 |
| 数据布局硬约束 | 写入/导入按 `episode_id` 分区（或粗粒度 bucket）+ 时间有序；视频独立 frames 表（M1 per-frame bytes，M2 GOP） |

## 2. 用户功能点 → 实现方式（M1）

| 用户功能 | 用户接口 | 实现落点 |
|---|---|---|
| 子集选择 | `EmbodiedDataset(scan, episodes=[...])` | episode → 元数据级 partition 裁剪；任意谓词 → Substrait filter 下推 |
| 多模态窗口样本 | `window={"state": (-10, 0), "action": (1, 10), "image": (-1, 0)}, stride, boundary` | 每 episode 读一次所需列，anchor + 行偏移切片；边界 skip/clamp |
| epoch 随机/可复现 | `set_epoch(e)` / `iter_epoch(e, rank, world_size)` | 单元顺序 = `default_rng([seed, epoch])` 置换；anchor 顺序 = `default_rng([seed, epoch, unit_idx])`；rank 交错取单元，各 rank 结果不重叠且并集为全集 |
| 视频读取 | `columns` 里含 image/frame bytes；`decode` 后置（M2） | M1 返回原始 bytes；解码在 torch adapter/collate 里由用户可选完成 |
| PyTorch 接入 | `lakesoul.embodied.torch.Dataset`（M1-2） | 逐样本 IterableDataset；worker 分片；shuffle buffer；与 `torch.distributed` 组合 |
| loader 性能 | 自动 | 修 `batch_readahead/fragment_readahead`、落实 `prefetch_size`（M1-4） |
| 存储效率 | 自动 | binary/blob 列默认不压缩 + Python 写路径透传 `vector_columns`（M1-3） |

## 3. M1 任务（实施中）

### M1-1 [Python] `lakesoul.embodied` 核心

- `Window`：行偏移窗口（相对 anchor，可为负），`boundary=skip|clamp`；
- `EmbodiedDataset(scan, window=..., stride=1, episodes=None, episode_column=None, seed=0)`：
  - 单元（unit）= scan partition（episode 分区时即 1 episode）；
  - 每单元读一次表，anchor 按 stride 生成并确定置换；
  - `iter_epoch(epoch, rank=None, world_size=None)` 产出 `{列名: np.ndarray}` 样本；
  - 置换由 `seed/epoch/unit_idx` 决定，跨 rank/进程一致，无需通信。
- 测试：与朴素 numpy 实现对齐、seed 可复现、rank 不重叠且全覆盖、边界 skip/clamp、窗口校验。

### M1-2 [Python] torch 适配（现有 `lakesoul/torch/dataset.py` 只做到"整批 yield + partition 分片"）

缺失项（详见实现时的分析）：
1. 逐样本产出与窗口拼装；
2. `set_epoch`/shuffle（含 shuffle buffer）；
3. `num_workers` 分片（现在是每个 worker 重复读同一 shard）；
4. 样本级/worker 级 seed 可复现与断点续训；
5. 张量转换与 collect/collate 约定；
6. 与 `torch.distributed` 的组合语义（rank × worker 两级分片）。

### M1-3 [Rust+Python] 存储策略（已完成）

- `ColumnPolicy`（`compress` / `row_block_size` / `data_block_target_bytes`）+ builder 接入 `LakeSoulIOConfig`；
- VortexSink 按列解析策略：vector 列沿用 1024 行 block，**binary/blob 列默认不压缩**（identity compressor），显式策略优先；
- Python 写路径全量透传 `vector_columns`：`IOConfig.vector_columns` → `_NativeWriter` → Rust，且 `write_arrow` / Ray / Daft 自动取表的 `vector_index_columns` 属性；
- 默认格式核对：Python/Rust 各写入路径均为 `vortex-compact`，无需改动。

### M1-4 [Python/Rust] loader 性能（已完成）

- Rust `prefetch_size` 从死配置变为真实预读：新增 `prefetch_size` option key，`LakeSoulReader::start` 用 `maybe_prefetch` 在后台任务里提前拉取 N 个 batch（`>=2` 生效，默认 1 保持原行为），带单测；
- Python `batch_readahead` 映射到 native `prefetch_size`（`Scanner.from_dataset/from_fragment`），`fragment_readahead` 接受并说明"所有 fragment 并发读取已天然满足"，非法值报 `ValueError`（不再 `NotImplementedError`）；
- `lakesoul.torch.Dataset` 现在把 scan 配置的 `batch_size` 传给 `to_batches`（此前被忽略）；
- `lakesoul.embodied.torch.Dataset` 新增 `prefetch=N`：后台线程保序预读样本，首个异常回传到消费端。

### M1-5 [Benchmark] 训练闭环（已完成）

- `python/examples/embodied/`：`synthetic.py`（每 episode 一分区、逐帧 image bytes）、
  `generate_data.py`（建表 + 导入）、`train.py`（窗口 + torch adapter + 线性模型训练循环）；
- `benchmark/embodied/run_benchmark.py`：`baseline_full / embodied_full /
  baseline_subset / embodied_subset / baseline_shuffle / embodied_shuffle /
  torch_loader` 七组，报告吞吐、读取字节、每样本字节、P50/P99；
- 本机跑通（8 episodes × 512 ticks，`vortex`，4KiB 随机图像）：
  - 全表：baseline 34.2k samples/s vs embodied 40.2k samples/s（约 1.2x）；
  - 子集（2/8 episodes）：读取 4.27 MB vs 17.08 MB（4x 剪枝），约 1.7x 提速；
  - `torch_loader`（DataLoader + shuffle buffer + prefetch）11.4k samples/s；
- 限制：数据在 page cache 中，读取对比偏乐观；本机磁盘高负载（12 MB/s fsync），
  写表耗时不进对比指标。

**M1 出口标准**：一条命令跑通"导入（合成）→ 采样 → 训练循环"并产出对比报告。

## 4. M2：Blob 外置透明化 + 导入器 + 自定义 Layout + 行级随机读

实施顺序调整：先做 **M2-4 导入器（Python 侧、用户可见、不依赖 blob 设计）**；
blob 外置（M2-1~3）因涉及跨引擎可见性与 pack GC/快照引用语义，先设计评审再接实现。

### M2-4 `import_lerobot` v3（已完成状态/动作 + 逐帧视频字节）

- `lakesoul.embodied.import_lerobot(source, table=..., path=...)`：读取本地 v3.0
  目录（`meta/info.json` + `meta/episodes/**/*.parquet`），按 `data_path`/`video_path`
  模板定位 shard；每 episode 一个分区、行内 `frame_index`/`timestamp` 有序；
- tabular 特征（含 `FixedSizeList`）按 LeRobot 名称去点（`observation.state` →
  `observation_state`）；视频用 PyAV 解码、Pillow 编码为逐帧 JPEG/PNG 字节；
- 支持 `episodes` / `cameras` / `include_video` / `overwrite`；v2.1 数据集明确报错；
- 依赖 `lakesoul[embodied]`（`av`、`pillow`）；GOP blob + frames 索引待 M2-1~3 落地后切换；
- 测试：合成 v3 数据集（含小 mp4）5 个用例（本机与 CI 均带 PG）。

### M2-1 ~ M2-3 Blob 外置（待设计评审）

- Blob 语义：`lakesoul.blob=auto|inline|external`；16KiB 内联 / 2MiB 外置 / pack 256MiB；`(uri, offset, len, crc)`；快照引用 + vacuum（专设计评审）；
- 透明读：默认批量物化 bytes（disk cache）；`BlobFile.read(offset, size)` 惰性路径；
- 自定义 Vortex BlobLayout（`file_format/vortex/layouts/blob.rs`，扩展注册）；
- 行级 range 下推 + `take`（只服务单样本/调试），与 blob range 共用寻址；
- `import_mcap`：与 `import_lerobot` 共用对齐/写入骨架；
- 视频解码 helper（`av`/`torchcodec`）已随 M2-4 提供基础版；
- 混合 benchmark：存储放大、GOP 随机读 P50、重写放大。

## 5. M3：时间语义与可复现

- 时间聚簇写入/compaction → 时间窗=行范围快路径；
- `align()/join_asof()`（导入未预对齐时的读时对齐）；
- 样本视图/manifest（跨快照稳定的行地址，才需要持久化）；
- Python 快照/版本参数（目前仅 Spark/Flink）；文档与示例收尾。

## 6. 验收指标

- **存储**：视频列相对逐帧 JPEG 的体积比；相对 MCAP topic-group 的读取放大；
- **访问**：单样本取数字节、range GET 次数、GOP 随机读 P50/P99（M2 起）；
- **训练**：loader 吞吐、GPU 利用率、shuffle 的随机性与可复现性、worker/rank 均衡；
- **运维**：改标注列的重写放大、GC 正确性、快照回滚后 blob 一致性（M2 起）。

## 7. 风险与约定

1. 自定义 Vortex layout 上游 API 尚在演进（当前 0.86）：先用现成 knobs，layout 按扩展注册；
2. Blob pack 的 GC 与快照引用语义需先定义：按快照 manifest 引用，vacuum 只清无引用 pack；
3. 透明物化会拷贝字节：默认路径要支持零拷贝 `BlobFile`，否则大视频列内存放大；
4. episode 长度不均时 rank 负载不均：导入按固定时长/行数切 chunk（硬约束的一部分）；
5. **DataLoader fork 风险**：父进程用过 native reader（tokio 线程）后再 fork worker 可能崩溃；
   示例/benchmark 默认 `num_workers=0`，`EmbodiedDataset` 已支持 pickle，可在 spawn 模式下使用；
6. `lakesoul-datafusion` 在 workspace 中 disabled，SQL 侧透传列策略需先确认启用路径。

## 8. 实施顺序

M1-1 → M1-2 → M1-3 → M1-4 → M1-5，每步带测试；M2、M3 依次跟进。
