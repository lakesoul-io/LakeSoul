# LakeSoul + Ray 整合功能正确性验证方案

---

## 1. 目标与范围

验证 `lakesoul.ray.read_lakesoul` 实现的 `ray.data.Datasource` 接口能 **正确、高效** 地将 LakeSoul 表数据暴露为 Ray Dataset，并与原生 Arrow reader (`lakesoul.arrow.lakesoul_dataset`) 行为一致。

**范围内：**
- 数据内容正确性（值、行数、列数、类型）
- Schema 一致性
- 不同并行度下的正确性
- 边界场景（空分区、特殊类型、非法参数）
- 与 Arrow reader 的性能基线对比

**范围外：**
- 多节点集群（本地模式验证）
- 写入操作（`do_write` 本身不支持）
- Spark/JVM 侧的正确性

---

## 2. 测试环境与前置条件

| 项目 | 说明 |
|------|------|
| 运行模式 | `ray.init("local")` |
| 数据准备 | 复用 "part" (20000 行 TPC-H) 和 "test_lfs" (1 行)；按需创建辅助表 |
| 依赖 | `pytest`, `ray[data]>=2.7`, `pyarrow` |
| 基准 | `lakesoul.arrow.lakesoul_dataset` — 同一套 Rust reader，预期数据完全一致 |

---

## 3. L1 — 功能正确性

### L1.1 行数验证

| 用例 | 输入 | 预期 |
|------|------|------|
| 标准表 | `part`, 默认参数 | 20000 行 |
| 单行表 | `test_lfs` | 1 行 |

### L1.2 Schema 验证

| 检查项 | 对比方式 |
|--------|---------|
| 列数 | `len(ds.schema())` vs Arrow version |
| 列名列表 | `[f.name for f in ds.schema()]` vs Arrow schema |
| 每列类型 | 逐字段比较 `pyarrow.DataType`（含 decimal 精度、嵌套类型） |

### L1.3 数据值全量对比

对 `part` 表的所有列，分别收集 Ray 和 Arrow reader 的全量值 → 排序 → 逐值 assert。确保不丢数据、不篡改值、无额外数据。

### L1.4 输出格式验证

| 格式 | 检查点 |
|------|--------|
| `ds.iter_rows()` | 逐行迭代计数 + 抽样对比 |
| `ds.iter_batches()` | 每批是 `pa.RecordBatch`，汇总行数正确 |
| Arrow 全量 | 与 `lakesoul_dataset().to_table()` 做 `pa.Table.equals()` |

---

## 4. L2 — 并行度验证

### L2.1 不同 parallelism 下的正确性

| parallelism | 验证 |
|-------------|------|
| 1 (串行) | 行数/schema/数据值不走样 |
| `len(data_files)` (默认) | 同上 |
| `len(data_files) * 2` (过订阅) | 同上 |
| 各值随机抽样 | vs Arrow 基准完全一致 |

### L2.2 Task 分布辅助检查

- `get_read_tasks(p)` 返回的 ReadTask 数量 == 数据文件数量
- 每个 ReadTask 的 `BlockMetadata.schema` 不为空

---

## 5. L3 — 边界与异常

### L3.1 数据边界

| 场景 | 预期 |
|------|------|
| 空分区 — 读无数据的分区 | 返回空 Dataset，不抛异常 |
| 大 Decimal — `p_retailprice` decimal128(15,2) | 值与 Arrow 一致，无精度丢失 |
| 全 NULL 列 | 类型保持，NULL 不变为默认值 |
| `retain_partition_columns` True/False | True 多分区列，False 不含 |

### L3.2 参数边界

| 参数 | 预期 |
|------|------|
| `batch_size=1` | 正确读全量 |
| `batch_size=10000` (>实际行数) | 正确 |
| `thread_count=0` | 底层报错，Ray 侧不吞异常 |
| `thread_count=8` | 正确读全量 |

### L3.3 异常场景

| 场景 | 预期 |
|------|------|
| `read_lakesoul("no_such_table")` | 明确异常，非 Ray 内部 500 |
| `do_write()` | `NotImplementedError` |
| 未装 ray 时 `import lakesoul` | 不报错，只有 import ray 才报 |

### L3.4 类型覆盖

对 LakeSoul 支持的 Arrow 类型各建小表：`int32/64`, `float32/64`, `string`, `decimal128`, `date32`, `timestamp`。每个验证类型保持、行数、抽样值 vs Arrow 基准。

---

## 6. L4 — 性能对比

### L6.1 吞吐量基线

| 指标 | 方法 |
|------|------|
| 对比对象 | `read_lakesoul(...).iter_batches()` vs `lakesoul_dataset(...).to_batches()` |
| 测量 | `time.perf_counter`, warmup 1 次后测 3 次取中位数 |
| 指标 | rows/sec, batch/sec, Ray overhead = Ray耗时 / Arrow耗时 |
| 阈值 | Ray overhead < 2x（Ray Data 有框架开销，但不应数量级恶化） |

## L6.2 并行度对性能影响

| parallelism | 记录 |
|-------------|------|
| 1 | 串行基线 |
| 文件数 | 默认并行 |
| 文件数 × 2 | 过订阅 |

本地模式下并行收益有限，但耗时不应明显超过串行。

---

## 7. L5 — 端到端 ML 使用案例：Kaggle Titanic（Ray 全流程）

本案例展示如何用 LakeSoul + Ray 完成一个 Kaggle 竞赛的完整 ML pipeline，
**无需 Spark 参与特征工程**——数据读取、预处理、训练全部在 Ray 生态内完成。

### 7.1 案例背景

| 项目 | 说明 |
|------|------|
| 数据集 | [Kaggle Titanic](https://www.kaggle.com/competitions/titanic) — 二分类，预测乘客是否生还 |
| 原始数据 | `train.csv`（891 行）, `test.csv`（418 行） |
| LakeSoul 表 | `titanic_raw`（Spark 导入 CSV → LakeSoul，与现有流程一致） |
| 目标 | 用 Ray Data 完成特征工程 + Ray Train 训练 DNN，全程不经过 Spark |

### 7.2 端到端流程

```
                    ┌─────────────────────────────┐
                    │  Spark (仅数据导入，一次性)     │
                    │  CSV → titanic_raw (LakeSoul) │
                    └─────────────┬───────────────┘
                                  │
            ┌─────────────────────▼────────────────────────┐
            │  Ray Data Pipeline（纯 Python，可重复迭代）     │
            │                                              │
            │  read_lakesoul("titanic_raw")                │
            │    → filter(split == "train")                │
            │    → map_batches(extract_title)              │
            │    → map_batches(categorize_family)          │
            │    → map_batches(impute_missing)             │
            │    → map_batches(one_hot_encode)             │
            │    → map_batches(vector_assemble)            │
            │    → random_split(90/10)                     │
            │    → materialize to LakeSoul (可选)           │
            └─────────────────────┬────────────────────────┘
                                  │
            ┌─────────────────────▼────────────────────────┐
            │  Ray Train                                   │
            │  TorchTrainer(DNN)                           │
            │    → 训练 → 验证 → 推理                        │
            └──────────────────────────────────────────────┘
```

### 7.3 代码骨架

```python
# examples/titanic_ray/train_ray.py
import ray
from ray.data import Dataset
from ray.train.torch import TorchTrainer
from lakesoul.ray import read_lakesoul

# ── Step 1: 从 LakeSoul 读取原始表 ──────────────────────
def load_data() -> Dataset:
    ds = read_lakesoul(
        "titanic_raw",
        batch_size=4096,
        partitions={"split": "train"},
    )
    return ds

# ── Step 2: Ray Data 特征工程（替代 Spark UDF） ─────────

def extract_title(batch: dict) -> dict:
    """从 Name 列提取 Title（Mr/Mrs/Miss/Rare）"""
    import re
    titles = []
    for name in batch["Name"]:
        match = re.search(r",\s*(\w+)\.", str(name))
        title = match.group(1) if match else "Unknown"
        if title in ["Lady", "Countess", "Capt", "Col", "Don",
                      "Dr", "Major", "Rev", "Sir", "Jonkheer",
                      "Dona", "Ms", "Mme", "Mlle"]:
            title = "Rare"
        titles.append(title)
    batch["Title"] = titles
    return batch


def categorize_family(batch: dict) -> dict:
    """根据 SibSp + Parch 生成 FamilySize 类别"""
    def _family(sibsp, parch):
        size = int(sibsp) + int(parch) + 1
        if size < 2:
            return "Single"
        elif size == 2:
            return "Couple"
        elif size <= 4:
            return "InterM"
        else:
            return "Large"

    batch["FamilySize"] = [
        _family(s, p)
        for s, p in zip(batch["SibSp"], batch["Parch"])
    ]
    return batch


def impute_missing(batch: dict) -> dict:
    """填充 Age 中位数、Embarked 众数"""
    import numpy as np
    ages = np.array([a for a in batch["Age"] if a is not None], dtype=float)
    median_age = np.median(ages) if len(ages) > 0 else 28.0
    batch["Age"] = [float(a) if a is not None else median_age for a in batch["Age"]]

    # Embarked 众数填充
    from collections import Counter
    embarked_vals = [e for e in batch["Embarked"] if e is not None]
    mode_emb = Counter(embarked_vals).most_common(1)[0][0] if embarked_vals else "S"
    batch["Embarked"] = [e if e is not None else mode_emb for e in batch["Embarked"]]

    # Fare 转为 float
    batch["Fare"] = [float(f) if f is not None else 0.0 for f in batch["Fare"]]
    return batch


def one_hot_and_assemble(batch: dict) -> dict:
    """独热编码 categorical 列 + 组装 feature vector"""
    import numpy as np

    categorical_maps = {
        "Pclass": ["1", "2", "3"],
        "Sex": ["male", "female"],
        "Embarked": ["C", "Q", "S"],
        "Title": ["Master", "Miss", "Mr", "Mrs", "Rare"],
        "FamilySize": ["Single", "Couple", "InterM", "Large"],
    }

    feature_vecs = []
    n = len(batch["Pclass"])
    for i in range(n):
        vec = []
        for col, categories in categorical_maps.items():
            val = str(batch[col][i])
            oh = [1.0 if val == cat else 0.0 for cat in categories]
            vec.extend(oh)
        # 数值特征
        vec.append(float(batch["Age"][i]))
        vec.append(float(batch["Fare"][i]))
        feature_vecs.append(vec)

    batch["features"] = np.array(feature_vecs, dtype=np.float32)
    batch["label"] = np.array(
        [int(s) for s in batch["Survived"]], dtype=np.int64
    )
    return batch


# ── Step 3: 组装 Pipeline ───────────────────────────────

def build_pipeline() -> Dataset:
    ds = load_data()
    ds = ds.map_batches(extract_title)
    ds = ds.map_batches(categorize_family)
    ds = ds.map_batches(impute_missing)
    ds = ds.map_batches(one_hot_and_assemble)
    # 只保留模型需要的列
    ds = ds.select_columns(["features", "label"])
    return ds


# ── Step 4: 训练函数 ────────────────────────────────────

def train_func(config: dict):
    """Ray Train worker 训练函数"""
    import torch
    import torch.nn as nn
    from ray.train.torch import get_device

    class TitanicDNN(nn.Module):
        def __init__(self, input_dim=26):
            super().__init__()
            self.bn = nn.BatchNorm1d(input_dim)
            self.fc1 = nn.Linear(input_dim, 256)
            self.fc2 = nn.Linear(256, 2)

        def forward(self, x):
            x = self.bn(x)
            x = torch.relu(self.fc1(x))
            x = self.fc2(x)
            return x

    model = TitanicDNN()
    device = get_device()
    model.to(device)

    train_loader = ray.train.get_dataset_shard("train")
    val_loader = ray.train.get_dataset_shard("val")

    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-2, weight_decay=5e-3)
    criterion = nn.CrossEntropyLoss()

    for epoch in range(config["num_epochs"]):
        model.train()
        for batch in train_loader.iter_torch_batches(batch_size=50):
            x = batch["features"].to(device)
            y = batch["label"].to(device)
            optimizer.zero_grad()
            loss = criterion(model(x), y)
            loss.backward()
            optimizer.step()

        # 验证
        model.eval()
        correct, total = 0, 0
        with torch.no_grad():
            for batch in val_loader.iter_torch_batches(batch_size=50):
                x = batch["features"].to(device)
                y = batch["label"].to(device)
                pred = model(x).argmax(dim=1)
                correct += (pred == y).sum().item()
                total += y.size(0)
        ray.train.report({"accuracy": correct / total, "epoch": epoch})


# ── Step 5: 主入口 ──────────────────────────────────────

def main():
    ray.init("local")

    # 特征工程
    ds = build_pipeline()
    train_ds, val_ds = ds.random_split([0.9, 0.1], seed=42)

    # Ray Train
    trainer = TorchTrainer(
        train_func,
        train_loop_config={"num_epochs": 50},
        datasets={"train": train_ds, "val": val_ds},
        scaling_config=ray.train.ScalingConfig(
            num_workers=1,
            use_gpu=False,
        ),
    )
    result = trainer.fit()
    print(f"Best accuracy: {result.metrics['accuracy']:.4f}")

    ray.shutdown()


if __name__ == "__main__":
    main()
```

### 7.4 验证要点

| 检查项 | 方法 | 成功标准 |
|--------|------|---------|
| 数据一致性 | Ray pipeline 输出 vs Spark `feature_transform.py` 输出 | 行数、列数、特征值完全一致 |
| 模型精度 | Ray Train 训练 vs PyTorch 原生训练（`train.py`） | 准确率差异 < 1% |
| 端到端可运行 | `python titanic_ray/train_ray.py` 一键执行 | 无异常，输出 accuracy |
| 无 Spark 依赖 | 全程不 import pyspark | 特征工程纯 Ray Data |
| 可复现 | 固定 seed，重复跑 3 次 | accuracy 标准差 < 0.5% |

### 7.5 与现有 Spark 流程的对比

| 维度 | Spark 流程 (`feature_transform.py`) | Ray 流程 (本案例) |
|------|--------------------------------------|-------------------|
| 特征工程 | PySpark UDF（需要 JVM） | Ray Data `map_batches`（纯 Python） |
| 调试体验 | 黑盒，Spark UI | 本地打断点，print 调试 |
| 迭代速度 | 每次改 UDF 需重新提交 Spark job | 修改即运行，秒级反馈 |
| 生态整合 | Spark MLlib | Ray Train、HuggingFace、XGBoost 等 |
| 部署复杂度 | 需要 Spark 集群 | `pip install lakesoul ray` 即可 |

---

## 8. 文件结构与 CI 集成

```
python/
├── tests/ray/
│   ├── conftest.py              # ray.init(local), 建辅助表 fixture
│   ├── test_l1_correctness.py   # 数据/Schema/值 全量对比
│   ├── test_l2_parallelism.py   # 不同 parallelism 正确性
│   ├── test_l3_boundary.py      # 空分区、类型、异常
│   ├── test_l4_performance.py   # 性能基线 (可选)
│   └── test_l5_e2e_titanic.py   # 端到端 Titanic 集成测试
│
└── examples/titanic_ray/
    ├── train_ray.py             # 端到端 Ray pipeline（上文骨架）
    └── README.md                # 使用说明
```

- **CI 默认**：跑 L1-L3 + L5（< 3min）
- **L4**：`@pytest.mark.slow`，`--run-performance` 显式触发
- **成功标准**：L1-L3 + L5 全 pass；L4 中 Ray/Arrow 耗时比 < 3x
