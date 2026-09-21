# LakeSoul 具身数据处理支持 · 执行计划

- 版本：v1.2（用户功能驱动版，M1 重排）
- 日期：2026-09-18
- 状态：M1 完成；M2 进行中（M2-4 导入器 + GOP 布局/读取已完成；blob 外置待设计评审）；M4-1a Daft 分布式导入（LeRobot frames）已完成
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

### M2-4b `import_mcap` v1（JSON 录制）

- 支持 JSON 与 protobuf 消息：protobuf 用 MCAP 内嵌 FileDescriptorSet 动态解码
  （覆盖 `foxglove.CompressedVideo` 等），bytes 保持 bytes、WKT `Timestamp` 转 float 秒；
- `columns={"observation_state": "topic[:field.path]"}` 映射 tabular 列，
  `cameras={"cam_high": "topic"}` 映射 base64 图像/视频帧；`row_topic` 定义行锚点，
  其他 topic 按最近邻 `tolerance` 秒对齐；类型从 JSON 值推断
  （标量 / FixedSizeList / string / bool）；
- 未知编码明确报错并列出可用 topic；测试：合成 MCAP（zstd chunk，JSON + protobuf）6 个用例；

### M2-4c GOP 视频布局（已完成）

- `import_lerobot(video_layout="gop")`：按关键帧把 mp4 demux 成自包含 Annex-B GOP，
  产出 `<table>`（ticks）+ `<table>_gops`（原始 packet、帧元数据）+ `<table>_frames`
  （frame_index → gop/position/offset/timestamp 索引），不逐帧解码/重编码，存储接近源体积；
- `lakesoul.embodied.video`：`demux_gops`（含 h264/hevc/vvc → Annex-B）、`decode_gop`、
  `decode_gop_range`（按 episode/camera 区间解码）、`encode_frames`（frames 模式共用）；
- 测试：合成 h264（keyint=4）→ 表结构 + 解码像素与源帧对齐；
- `EmbodiedDataset(video=GopVideo(gops, frames), video_window=...)`：窗口行区间 → 帧区间解码，
  每个 unit 按 (episode, camera, gop) 缓存已解码帧；`video_window` 支持列名或 (start, end)，
  默认跟随第一个窗口列；检测相机列与窗口列重名并报错；
- `import_mcap(video_layout="gop")`：按 access unit 的 NAL 关键帧分组 Annex-B GOP，
  使用 channel `sequence` 保证 decode order（无 sequence 时按 log_time 回退，B 帧流需录制端写 sequence），
  产出与 LeRobot 同构的 `<table>_gops` / `<table>_frames`，`GopVideo` 直接消费；
  JPEG/PNG 相机提示改用 `video_layout="frames"`；
- 数据集侧测试：fake video 单测（窗口/边界/重名/分区校验、pickle）+ PG 端到端
  （LeRobot/MCAP GOP 导入 → 直接进窗口样本）。
- GOP blob + 帧索引待 M2-1~3 blob 外置落地后接入。

### M2-1 ~ M2-3 Blob 外置（实施中）

- 已定决策：opt-in 表属性 `blob_columns`（列 → `mode/threshold/pack_target`）→ IOConfig options；
  tagged binary 行内表示；pack 跟随数据文件（`<data_file>.<column>.blob`，删数据文件即清理）；
  仅 Python/native 路径；阈值 16KiB inline / 2MiB external / pack 目标 256MiB；`LAKESOUL_BLOB_DISABLE` 逃生；
- 已完成（端到端）：
  - Rust codec（`blob.rs`）与 writer 接线（`write_record_batch` 编码、`flush` 落
    `<data_file>.<column>.blob`）；
  - reader 物化（`BlobMaterializer`：object store range 读 + CRC/length 校验 + moka 缓存）；
  - Python 透传（`create_table` 校验 `blob_columns`；`write_arrow`/Ray/Daft 注入写选项；
    scan 注入 reader 选项）；
  - GOP 外置 e2e：分区表带 `blob_columns={"data": external}` 时每数据文件一个 pack，
    `GopVideo`/`EmbodiedDataset` 直接解码；
  - benchmark `--with-blob` 对比列；
- 待办：pack GC/vacuum、SQL 引擎 glue、零拷贝 `BlobFile`（M2-2 后续）；
- 原 Blob 语义：`lakesoul.blob=auto|inline|external`；16KiB 内联 / 2MiB 外置 / pack 256MiB；`(uri, offset, len, crc)`；快照引用 + vacuum（专设计评审）；
- 透明读：默认批量物化 bytes（disk cache）；`BlobFile.read(offset, size)` 惰性路径；
- 自定义 Vortex BlobLayout（`file_format/vortex/layouts/blob.rs`，扩展注册）；
- 行级 range 下推 + `take`（只服务单样本/调试），与 blob range 共用寻址；
- `import_mcap`：与 `import_lerobot` 共用对齐/写入骨架；
- 视频解码 helper（`av`/`torchcodec`）已随 M2-4 提供基础版；
- 混合 benchmark：存储放大、GOP 随机读 P50、重写放大。

### M2-5a 视频布局存储/读取量化（已完成）

- `benchmark/embodied/run_video_layout_benchmark.py` + `lerobot_source.py`：
  生成渐变图案的 LeRobot v3 源（mp4 + parquet），分别以 frames（逐帧 JPEG）、gop、
  daft-frames（native runner）导入，报告导入吞吐、磁盘体积、窗口采样吞吐与
  GOP 解码 P50/P99；
- 示例结果（8 episodes × 120 ticks，128×128，keyint=16，vortex）：
  - 存储：源 mp4 0.16 MB；frames 3.55 MB（21.6x mp4）；gop 0.42 MB（2.54x mp4），
    **gop 比 frames 小 8.5x**（源为低熵渐变，真实视频差距视码率而定）；
  - 导入：frames 0.92s / 1.0k rows/s；gop 0.20s / 4.9k rows/s；
    daft-frames native runner 2.62s（单进程，仅体现引擎开销）；
  - 读取：frames 8.1k samples/s、15.3 KB/sample；gop 216 samples/s（sample P50
    0.057 ms，命中已解码 GOP 缓存；P99 25.6 ms 为 unit 内首个 GOP 解码），
    1.8 KB/sample，整 GOP 解码 P50 14 ms；
- 结论：GOP 布局把存储压到接近源体积（8.5x 收益），代价是采样时需解码
  （缓存命中时很快）；blob 外置主要收益在把 frames 的逐帧字节或 GOP 数据移出行，
  阈值仍按 16KiB inline / 2MiB external 设计，pack 跟随数据文件。

## 5. M4：Daft 分布式（进行中）

原则（已确认）：混合方案——表格/逐帧用 Daft 原生 `daft.datasets.lerobot`，GOP 与
MCAP protobuf 用本仓 `@daft.cls` actor；先在 Daft native runner 上保证正确，
Ray runner 只要求接口兼容（gated 测试）。

### M4-1a `import_lerobot`（Daft，frames 布局，已完成）

- `lakesoul.embodied.daft.import_lerobot`：`daft.datasets.lerobot.read` 扫描/解码视频，
  列名去点、image 列用 `encode_image` 转 bytes、`episode_id` 补零为 `ep000000`，
  然后一次 `write_daft`（worker 写文件、driver 单次提交）；
- `episodes` / `cameras` / `include_video` / `sort_rows`（默认按
  `episode_id + frame_index` 排序，保证窗口行偏移与 `frame_index` 一致）/ `overwrite`；
- 与单机导入复用同一套 schema/feature 解析，表结构一致；
- 测试：native runner 上合成 v3 数据集（含 mp4）3 个用例（行数/分区/JPEG 帧/窗口/覆盖保护）。

### M4-1b LeRobot GOP（Daft，已完成）

- `lakesoul.embodied.daft.import_lerobot_gop`：ticks 表仍由
  `daft.datasets.lerobot` 生成；`_gops` / `_frames` 由一个 `@daft.cls` actor
  按 (episode, camera) 生成——actor 在 worker 内按 video 文件缓存 `demux_gops`
  结果，用 `select_episode_frames` 切出 episode 区间并重新编号 GOP，返回
  `list<struct>` 后 `explode` 展开，再经 `write_daft` 写入；
- 复用 `GOPS_SCHEMA` / `FRAMES_SCHEMA` 与纯 helper，语义与单机 gop 布局一致；
- 修复 `lakesoul/daft/sink.py` 的类型兼容：允许 list↔large_list（Daft 的
  `List` 在 Arrow 中是 `large_list`），否则任何 list 列都无法通过 Daft 写入；
- 测试：native runner 上 GOP 三表 + `GopVideo` 解码 + 数据集窗口。

### M4-2a 分布式窗口采样（已完成）

- `lakesoul.embodied.daft.read_samples(scan, window=..., stride=..., order_by="frame_index", seed, epoch)`：
  逐 episode 排序 + `groupby` 聚合到单 worker，用 `daft.func` UDF 打包窗口
  （anchor 置换用 `seed/epoch`，与单机同规则）后 `explode`，返回
  `episode_id / anchor / 每个窗口列（list）` 的 lazy DataFrame；
- 与 `EmbodiedDataset` 语义一致（行偏移窗口、`boundary="skip"`），测试逐样本对齐
  （stride=1/2 均比对通过）；`clamp` 明确报错待补；
- 限制：尚未包含 GOP 图像解码与样本级 rank 分片（Daft 自身调度负责并行）。

### M4-2b 分布式 GOP 帧解码（已完成）

- `lakesoul.embodied.daft.read_gop_frames(gops_scan, frames_scan, cameras=..., image_format=...)`：
  每个 GOP 在 worker 内 `decode_gop` 一次，explode 后与 frames 索引 join，
  输出 `episode_id/camera/frame_index/timestamp/width/height/image`；
  `image_format="JPEG"/"PNG"` 输出编码字节，`None` 输出原始 RGB；
- 测试：逐帧像素与单机 `GopVideo` 对齐、秒级时间戳、原始 RGB 尺寸校验。

### M4-1c MCAP 分布式（已完成，frames 布局）

- `lakesoul.embodied.daft.import_mcap(source, ...)`：`source` 可为文件、目录（`*.mcap`）
  或文件列表；每个文件一个 task，由 `mcap.build_frame_episode`（新抽出的纯函数，
  支持 JSON + protobuf、与单机同语义）构建行，`func(return_dtype=...)` 返回
  `list<struct>`，explode 后一次 `write_daft` 提交；
- 复用单机 schema/类型（fixed-size list 逐层映射到 Daft 类型），表结构与单机一致；
- 限制：仅 frames 布局；MCAP 的 GOP 布局仍走单机导入（`video_layout="gop"`）。

### M4-3 Ray / native runner（已完成）

- native runner 单测已覆盖（M4-1a/b/c、M4-2a/b）；
- 新增 `tests/embodied/test_daft_ray.py`（`LAKESOUL_DAFT_RAY_TEST=1`）：Ray runner 下
  跑 LeRobot frames 导入、`read_samples` 与单机逐样本对齐、GOP 导入 +
  `read_gop_frames` 像素/时间戳校验，本地 8s 通过；
- `tests/vector/test_daft_ray_distribution.py` 的过期 import 修复后本地验证：
  12 shards 分布到 4 个 executor。

## 6. M3：时间语义与可复现

- 时间聚簇写入/compaction → 时间窗=行范围快路径；
- `align()/join_asof()`（导入未预对齐时的读时对齐）；
- 样本视图/manifest（跨快照稳定的行地址，才需要持久化）；
- Python 快照/版本参数（目前仅 Spark/Flink）；文档与示例收尾。

## 7. 验收指标

- **存储**：视频列相对逐帧 JPEG 的体积比；相对 MCAP topic-group 的读取放大；
- **访问**：单样本取数字节、range GET 次数、GOP 随机读 P50/P99（M2 起）；
- **训练**：loader 吞吐、GPU 利用率、shuffle 的随机性与可复现性、worker/rank 均衡；
- **运维**：改标注列的重写放大、GC 正确性、快照回滚后 blob 一致性（M2 起）。

## 8. 风险与约定

1. 自定义 Vortex layout 上游 API 尚在演进（当前 0.86）：先用现成 knobs，layout 按扩展注册；
2. Blob pack 的 GC 与快照引用语义需先定义：按快照 manifest 引用，vacuum 只清无引用 pack；
3. 透明物化会拷贝字节：默认路径要支持零拷贝 `BlobFile`，否则大视频列内存放大；
4. episode 长度不均时 rank 负载不均：导入按固定时长/行数切 chunk（硬约束的一部分）；
5. **DataLoader fork 风险**：父进程用过 native reader（tokio 线程）后再 fork worker 可能崩溃；
   示例/benchmark 默认 `num_workers=0`，`EmbodiedDataset` 已支持 pickle，可在 spawn 模式下使用；
6. `lakesoul-datafusion` 在 workspace 中 disabled，SQL 侧透传列策略需先确认启用路径。

## 9. 实施顺序

M1-1 → M1-2 → M1-3 → M1-4 → M1-5，每步带测试；随后 M2（导入器/GOP/blob）、M4（Daft 分布式）、M3 依次跟进。
