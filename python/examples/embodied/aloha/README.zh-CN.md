# 使用 LakeSoul 和 Daft 的 ALOHA Frames 数据处理流程

本示例导入 `lerobot/aloha_static_thread_velcro` 数据集，并将其 Frames 表处理成质量报告、筛选区间、数据集划分、Prepared 表和训练样本索引。

这些脚本是 LakeSoul API 的应用示例，不是新的库接口。导入使用 `lakesoul.embodied.daft.import_lerobot()`；QC 和数据准备使用 `LakeSoulCatalog`、`scan().to_daft()` 和 `write_daft()`。

| 文件 | 用途 |
| --- | --- |
| `import_raw.py` | 导入指定 Episode 和两路相机，核对行数并抽查实际记录。 |
| `prepare_dataset.py` | 全量 QC、区间筛选、Episode 划分、Prepared 写入、样本索引生成和样本回读。 |
| `common.py` | 共用的字段名、参数辅助函数和 Catalog 连接配置。 |

## 运行环境

安装的 LakeSoul 需要包含机器人数据的 Daft 导入接口，以及 Daft Sink 中对嵌套列表、字符串类型的兼容转换。PostgreSQL 元数据库结构必须与安装的 LakeSoul 版本匹配，包括必要的仓库数据库迁移。

导入需要 Daft 提供 `daft.datasets.lerobot.read` 接口。本地导入使用过 Daft 0.7.25，0.7.15 不包含该接口。1
需要安装 NumPy、PyArrow 和 Pillow；导入阶段还需要 PyAV。Ray Worker 需要相同的 LakeSoul 和兼容的依赖。

运行前配置连接信息，将示例地址和凭据替换为实际值：

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME='lakesoul_test'
export LAKESOUL_PG_PASSWORD='<密码>'
export AWS_ENDPOINT='http://127.0.0.1:9000'
export AWS_ACCESS_KEY_ID='<访问密钥>'
export AWS_SECRET_ACCESS_KEY='<秘密密钥>'
```

使用 Ray 时，地址必须能够被参与执行的所有 Pod 或节点访问。这里的 localhost 地址仅适用于本机运行示例。

## 1. 导入 Raw 数据

激活所需的 Python 环境，在 LakeSoul 仓库根目录执行。如果本地没有数据集，先下载 LeRobot v3 数据集：

```bash
hf download lerobot/aloha_static_thread_velcro --repo-type dataset \
  --local-dir /mnt/robot-data/aloha_static_thread_velcro
```

选择新的表名、存储路径和导入报告目录：

```bash
python python/examples/embodied/aloha/import_raw.py \
  --source /mnt/robot-data/aloha_static_thread_velcro \
  --namespace default \
  --episodes 0,1,2,3,4,5,6,7,8,9 \
  --cameras cam_high,cam_left_wrist \
  --raw-table aloha_velcro_raw_v1 \
  --raw-path s3://YOUR_BUCKET/embodied/aloha_velcro_raw_v1 \
  --runner native \
  --output /tmp/aloha-import-v1
```

导入程序要求源数据为 50 FPS，状态和动作均为 14 维向量，图像尺寸为 640 × 480。图像以 JPEG 字节形式保存在 Raw 表中。
保留 `/tmp/aloha-import-v1/manifest.json`：其中记录的源 Episode 长度是后续 QC 使用的独立依据。导入阶段核对各 Episode 的行数，并抽查 5 行，不代表已经完成全量质量检查。

## 2. QC、Prepared 和样本索引

`prepare_dataset.py` 是主要处理脚本，由 `run()` 串联以下步骤，输入是已经导入的 Raw 表，不会重新导入源视频。
`load_configuration()` 从导入清单读取预期 Episode 长度，从 `info.json` 读取向量维度和相机尺寸，从 `tasks.parquet` 读取任务引用，作为质量检查依据。

```text
Raw + 来源元数据
  → 帧级和 Episode 级质量检查
  → 质量区间 + 可选人工行为标注 → 保留区间
  → 按 Episode 划分 Train/Val/Test
  → Prepared 数据
  → 观察窗口和动作窗口 → Samples 索引 → 样本回读
```

### 处理步骤

1. **检查数据质量：`inspect_frame()`、`check_row()`、`check_episode()`。**
   通过 Daft 读取 Raw，检查 state/action 维度及数值是否有限、图像能否解码及尺寸、时间戳和任务引用；再按 Episode 汇总，检查行数、缺帧、重复帧和时间戳顺序。
   问题明细写入 `issues.json`，Episode 汇总写入 `quality.json`，为后续判断哪些数据不可用提供依据。

2. **识别并筛选可用区间：`build_quality_intervals()`、`apply_behavior_annotations()`。**
   将能定位的质量问题转换为无效帧区间；无法可靠局部保留时，隔离整个 Episode。
   如果提供人工标注，再按标注边界拆分区间，根据排除标签决定是否选用。无效区间始终排除；未标注但质量有效的区间标为 `unknown`，默认保留。
   `intervals.json` 同时记录质量、行为、是否选用及原因，也保留被排除的区间。连续保留片段分配 `valid_interval_id`，防止后续窗口跨越缺口。

3. **划分 Train/Val/Test：`prepare_dataset()` 中调用 `split_episodes()`。**
   对存在保留区间的 Episode ID 使用固定随机种子打乱，再按配置比例划分，也可以通过 `--split-source` 复用已有划分。
   同一 Episode 的所有区间属于同一个集合，避免重叠轨迹窗口同时进入训练集和评估集。结果保存为 `splits.json`。

4. **生成 Prepared：`select_prepared_rows()`、`prepare_dataset()`。**
   使用 Daft 将 Raw 与保留区间、Split 清单关联，只提取选中的帧，保留原始帧号、时间戳和各模态数据，并增加 `split`、`valid_interval_id`。
   将包含 JPEG 字节的实际记录写入 LakeSoul Prepared 表。这一步不做数值归一化。

5. **构造训练窗口：`generate_training_samples()`、`build_sample_index()`。**
   调用 `lakesoul.embodied.daft.read_samples()`，在每个连续保留区间内构造窗口。默认规则下，锚点为 `t` 时，观察取 `t-9` 到 `t`，动作取 `t` 到 `t+9`。
   状态和两路相机使用相同的观察帧，组合成多模态样本。窗口不足直接跳过；必需字段缺失、帧序列或 Episode/Split 不一致的窗口不生成索引。
   Samples 表保存窗口范围和来源引用，图像内容仍存放在 Prepared 中，不重复写入。

6. **回读样本预览：`read_training_sample()`。**
   根据样本索引回读 Prepared，将窗口中的状态向量、两路相机 JPEG 序列和动作向量按顺序组织成 Python 字典。
   `--preview-samples` 控制检查多少个样本。最终 `qc_manifest.json` 汇总输出引用、质量结果、数据集划分数量、采样规则及预览长度。
   这一步验证实际样本能否读出，尚未生成 Tensor Batch，也不执行模型训练。

### 执行命令

使用实际的 Raw 表及其对应的导入清单。将 `YOUR_BUCKET` 替换为实际桶名：

```bash
python python/examples/embodied/aloha/prepare_dataset.py \
  --namespace default \
  --raw-table aloha_velcro_raw_v1 \
  --raw-metadata /tmp/aloha-import-v1/manifest.json \
  --info-json /mnt/robot-data/aloha_static_thread_velcro/meta/info.json \
  --tasks /mnt/robot-data/aloha_static_thread_velcro/meta/tasks.parquet \
  --prepared-table aloha_velcro_prepared_v1 \
  --prepared-path s3://YOUR_BUCKET/embodied/aloha_velcro_prepared_v1 \
  --samples-table aloha_velcro_samples_v1 \
  --samples-path s3://YOUR_BUCKET/embodied/aloha_velcro_samples_v1 \
  --validation-ratio 0.1 --test-ratio 0.1 --seed 42 \
  --observation-steps 10 --action-steps 10 --action-offset 0 --stride 5 \
  --preview-samples 1 --runner native \
  --output /tmp/aloha-qc-v1
```

只创建两张输出表：

- Prepared：筛选后的 Raw 记录，增加 `split` 和 `valid_interval_id`。
- Samples：引用 Prepared 的窗口索引，不重复保存图像内容。

中间结果保存为 `--output` 目录下的 JSON 文件，不再创建 LakeSoul 表：

- `issues.json`：字段级问题及受影响的帧区间。
- `quality.json`：每个 Episode 的行数和 PASS/HAS_ERRORS 汇总。
- `intervals.json`：各区间的质量、行为、选择结果和原因。
- `splits.json`：Episode 到 Train、Val、Test 的归属关系。

这些文件在后续步骤完成前写出，即使 Prepared 或样本生成失败，也可以查看已经生成的质量报告。
每次运行建议使用独立报告目录；已有目录中的同名报告文件会覆盖。

运行报告保存在 `/tmp/aloha-qc-v1/qc_manifest.json`。脚本会新建输出表，不会覆盖已有表。失败的运行可能留下部分表，因此重新运行时使用新的输出表名和路径。Raw 不需要重新导入。

没有人工标注时，质量有效的区间会保留，行为标签为 `unknown`。
可选的人工标注 JSON：

```json
[
  {
    "episode_id": "ep000003",
    "start_frame": 0,
    "end_frame_exclusive": 40,
    "behavior": "preparation",
    "reason": "人工确认的起始准备阶段"
  }
]
```

通过 `--behavior-annotations annotations.json --exclude-behaviors preparation,idle` 使用。
这些标签由用户提供，脚本不会自动识别。质量无效的区间始终排除。修改标注后，如果需要保持原来的 Episode 数据集归属，可通过 `--split-source /路径/splits.json` 指定原划分文件。

## 3. 在 Ray 上执行

在已有 Ray Job 中使用 `--runner ray --ray-address auto`。
任务工作目录根部需要包含：

```text
prepare_dataset.py
common.py
manifest.json
info.json
tasks.parquet
```

元数据参数使用相对路径：
`--raw-metadata manifest.json --info-json info.json --tasks tasks.parquet`。
Worker 从 LakeSoul 读取图像字节，因此 QC 不需要原始 MP4。若执行分布式导入，则 Worker 需要能够访问源数据集。

Daft 的扫描、UDF 检查、聚合、关联、窗口构造和写入使用 Ray runner。
窗口构造调用 `lakesoul.embodied.daft.read_samples`，并设置 `episode_column="valid_interval_id"`，因此窗口不会跨越筛选后的有效区间边界。
区间处理、数据集划分，以及将窗口元数据转换成持久化样本索引的逻辑，在 Driver 中执行。
任务和 Worker 都需要正确的连接环境。报告目录属于任务 Driver 所在节点。

使用持久化报告目录，或在节点删除前复制出 JSON 报告。新 Ray Job 需要复用划分时，将 `splits.json` 一起放进工作包，并传入 `--split-source splits.json`。

## 4. 读取一个样本

在示例目录中，使用相同的连接环境执行：

```python
from daft import col
from common import catalog_from_environment
from prepare_dataset import read_training_sample

catalog = catalog_from_environment("default")
indices = catalog.table("aloha_velcro_samples_v1").scan().to_daft()
rows = indices.where(col("split") == "train").limit(1).to_arrow().to_pylist()
if rows:
    sample = read_training_sample(catalog, rows[0])
    print({key: len(value) for key, value in sample["observation"].items()})
    print("action", len(sample["action"]))
```

观察数据包含状态向量和 JPEG 字节列表，动作数据包含目标动作向量。
窗口始终位于同一个 Episode、同一个连续保留区间内，不做填充。
`stride` 控制锚点移动步长，不改变窗口内部相邻帧的间隔。

当观察、动作长度均为 10，动作偏移为 0 时，传给 `read_samples` 的窗口配置为：
各观察字段使用 `(-9, 1)`，动作使用 `(0, 10)`。结束偏移不包含在窗口中。
用于检查的元数据窗口覆盖观察和动作的整个跨度。索引构造会排除缺帧、重复帧，以及 Episode 或 Split 不一致的窗口。
所需模态为空的样本，在 Worker 上过滤后才收集轻量元数据。

锚点遵循接口规则：从每个区间的第 0 行开始，按 `stride` 选择候选位置，然后跳过越界窗口。
对于 `[0,700)`、观察和动作长度 10/10、偏移 0、步长 5，锚点为 `10,15,...,690`，共 137 个样本。
旧的自定义实现使用 `9,14,...,689`；其他区间长度下，两种规则的样本数量也可能不同。
接口会打乱候选顺序；保存索引时按 Episode、区间和锚点排序，使预览结果稳定。报告会记录采样接口和锚点规则。

`read_samples` 会在一个 Worker 上聚合一个区间，包括所选图像字段，并在该 Worker 上构造重叠窗口。
图像不会收集到 Driver，也不会重复写入索引表，但 Worker 内存占用会随区间大小和窗口重叠程度增长。
这适用于当前小规模 ALOHA Demo，不是流式训练 DataLoader。

本示例不计算归一化统计量、不组装 Tensor Batch、不训练模型、不固定数据库快照，也不核对源视频的时间对应关系。
处理和读取索引期间，应保持输入与输出数据不变。
