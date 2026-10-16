"""从 LakeSoul 读取一批 ALOHA 样本，转换为模型可以接收的 CPU Tensor。

执行顺序：Samples 表读取索引 → 回读 Prepared 中的窗口数据 → 解码图像
→ 可选的 state/action 归一化 → 堆叠 Batch → 打印形状、可选保存 .pt。
本脚本不重新筛选轨迹或构造窗口，也不执行模型训练。
它直接读取 LakeSoul，不需要先生成其他 Batch 文件。

Run beside prepare_dataset.py; requires torch, numpy and Pillow in addition to
the preparation dependencies. Uses the same LakeSoul/S3 environment variables.

Images become float32 RGB [B, X, 3, H, W] in [0, 1]. State/action stay in
source units unless --stats supplies training-only mean/std, in this format:
{"observation_state": {"mean": [...], "std": [...]},
 "action": {"mean": [...], "std": [...]}}
Zero standard deviations use a divisor of 1. Statistics are never fitted here.
This is a small readback example, not a high-throughput training DataLoader.
"""

import argparse
import io
import json
from pathlib import Path

import numpy as np
from PIL import Image


def parse_cameras(value):
    """显式选相机；none 表示只读 state/action，省略参数则沿用样本字段。"""
    if value.strip().lower() == "none":
        return []
    cameras = [name.strip() for name in value.split(",")]
    if not all(cameras) or len(set(cameras)) != len(cameras):
        raise argparse.ArgumentTypeError(
            "Use unique comma-separated camera names, or none"
        )
    if any(not name.startswith("cam_") for name in cameras):
        raise argparse.ArgumentTypeError(
            "Camera names must start with cam_, e.g. cam_high"
        )
    return cameras


def select_camera_fields(index, cameras):
    """在数据库读取前裁剪投影，不修改 Samples 表或原始索引。"""
    if cameras is None:
        return index
    fields = ["observation_state", *cameras]
    missing = set(fields) - set(index["observation_fields"])
    if missing:
        raise ValueError(
            f"Sample {index.get('sample_id')} lacks requested fields: {sorted(missing)}"
        )
    return {**index, "observation_fields": fields}


def decode_images(images, image_size):
    """单个样本、单路相机：X 份 JPEG 字节 → [X, 3, H, W] 图像数组。"""
    frames = []
    # 输入已按观察帧的时间顺序排列；逐张转换，保持原来的顺序。
    for data in images:
        # BytesIO 将数据库中的 JPEG 字节包装为 Pillow 可以读取的内存文件。
        with Image.open(io.BytesIO(data)) as image:
            # 统一为 RGB，并直接缩放。image_size 的顺序是 (宽, 高)。
            rgb = image.convert("RGB").resize(image_size, Image.BILINEAR)
            # HWC → CHW；像素转 float32 并从 0～255 缩放到 0～1。
            frames.append(np.asarray(rgb, dtype=np.float32).transpose(2, 0, 1) / 255.0)
    # 新增时间维度 X；这里只堆同一个样本的帧，还没有 Batch 维度。
    return np.stack(frames)


def normalize_vectors(values, stats):
    """状态或动作序列转 float32；提供统计量时逐维做 (值 - 均值) / 标准差。"""
    values = np.asarray(values, dtype=np.float32)
    # 不提供 --stats 时保留原始单位，只改变数值存储类型。
    if stats is None:
        return values
    # 例如 ALOHA 的 14 维向量，需要 14 个 mean 和 14 个 std。
    # 同一组统计量用于窗口内的所有时间步，且必须事先仅用训练数据计算。
    mean = np.asarray(stats["mean"], dtype=np.float32)
    std = np.asarray(stats["std"], dtype=np.float32)
    if mean.shape != (values.shape[-1],) or std.shape != mean.shape:
        raise ValueError("Statistics must contain one mean/std per vector dimension")
    if not np.isfinite(mean).all() or not np.isfinite(std).all() or (std < 0).any():
        raise ValueError("Statistics must be finite with non-negative std")
    # 常量维度 std=0 时用 1 作除数，避免除零；这里不重新计算统计量。
    return (values - mean) / np.where(std == 0, 1.0, std)


def build_batch(samples, image_size=(224, 224), stats=None):
    """多个样本字典 → 一个包含状态、各路图像、动作和来源信息的 Batch。

    B：实际样本数，X：观察步数，Y：动作步数，D：向量维度（当前为 14）。
    state [B,X,D]；每路图像 [B,X,3,H,W]；action [B,Y,D]。
    X/Y 来自已有样本窗口；相机字段沿用 Samples 指定的观察字段。
    """
    import torch

    if not samples:
        raise ValueError("Cannot build an empty batch")
    # read_training_sample 将向量映射到 state，其余观察字段为相机 JPEG 序列。
    cameras = tuple(key for key in samples[0]["observation"] if key != "state")
    expected_fields = {"state", *cameras}
    observations = []
    actions = []
    # 先分别处理每个样本。state 与图像属于观察窗口，action 属于动作目标窗口。
    for sample in samples:
        observation = sample["observation"]
        if set(observation) != expected_fields:
            raise ValueError("Samples in a batch must have the same observation fields")
        observations.append(
            {
                "state": normalize_vectors(
                    observation["state"], stats["observation_state"] if stats else None
                ),
                **{
                    camera: decode_images(observation[camera], image_size)
                    for camera in cameras
                },
            }
        )
        actions.append(
            normalize_vectors(sample["action"], stats["action"] if stats else None)
        )
    # np.stack 新增最外层 B 维度，要求各样本对应字段的形状相同。
    # torch.from_numpy 转成 CPU Tensor；没有使用 GPU，也没有调用模型。
    return {
        "observation": {
            key: torch.from_numpy(np.stack([item[key] for item in observations]))
            for key in ("state", *cameras)
        },
        "action": torch.from_numpy(np.stack(actions)),
        # 来源信息保持为 Python 字典列表，便于回查 episode、锚点和窗口范围。
        "metadata": [sample["metadata"] for sample in samples],
    }


def main():
    # 第一步：读取参数。默认取 train 的 4 个样本，图像缩放为 224×224。
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--samples-table", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--split", choices=("train", "val", "test"), default="train")
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument(
        "--cameras",
        type=parse_cameras,
        help="Comma-separated camera columns; default: all cameras in Samples. Use none for state/action only.",
    )
    parser.add_argument(
        "--image-size",
        type=int,
        nargs=2,
        default=(224, 224),
        metavar=("WIDTH", "HEIGHT"),
    )
    parser.add_argument(
        "--stats",
        type=Path,
        help="Optional training-only mean/std JSON; see module docstring",
    )
    parser.add_argument(
        "--output", type=Path, help="Optional .pt file containing tensors and metadata"
    )
    args = parser.parse_args()
    if args.batch_size < 1 or min(args.image_size) < 1:
        parser.error("batch-size and image-size must be positive")

    from common import catalog_from_environment
    from prepare_dataset import read_training_sample
    from daft import col
    import torch

    # 第二步：使用与 prepare_dataset.py 相同的 PG/S3 环境变量连接 LakeSoul。
    catalog = catalog_from_environment(args.namespace)
    # Samples 表存的是窗口索引。按 ID 排序取前 N 个，不做随机采样或重新划分。
    # 少于 N 个时使用实际数量；没有样本则报错。
    indices = (
        catalog.table(args.samples_table)
        .scan()
        .to_daft()
        .where(col("split") == args.split)
        .sort("sample_id")
        .limit(args.batch_size)
        .to_arrow()
        .to_pylist()
    )
    if not indices:
        raise ValueError(f"No {args.split} samples in {args.samples_table}")
    # 第三步：复用 prepare_dataset.py 的读取函数。
    # 索引包含 Prepared 表引用、episode、有效区间、观察和动作窗口范围。
    # 根据这些信息读取实际记录，返回 state 序列、各路 JPEG 序列、action 序列。
    # 因此命令只需给 Samples 表名，无需再次指定 Prepared 表或窗口长度。
    # 先校验整批并裁剪 observation_fields：未选相机不进入 Prepared 查询。
    indices = [select_camera_fields(index, args.cameras) for index in indices]
    samples = [read_training_sample(catalog, index) for index in indices]
    # 第四步：可选读取训练集统计量；不提供时 state/action 不归一化。
    stats = json.loads(args.stats.read_text()) if args.stats else None
    # 第五步：解码、转换数值类型并堆叠成一批 CPU Tensor。
    batch = build_batch(samples, tuple(args.image_size), stats)
    # 第六步：打印形状供检查。形状正确不等于图像和时间对应关系已经验收。
    for name, tensor in {**batch["observation"], "action": batch["action"]}.items():
        print(f"{name}: shape={list(tensor.shape)}, dtype={tensor.dtype}")
    print(f"Samples: {len(indices)}; split: {args.split}")
    print(
        "State/action: "
        + ("z-score normalized" if stats else "original values (not normalized)")
    )
    print("Images: RGB float32 in [0, 1]; dimensions: B, X, C, H, W")
    if args.output:
        # 第七步（可选）：保存当前这一批数据，不是保存训练后的模型权重。
        # 可用 torch.load("sample_batch.pt", weights_only=True) 回读。
        # 同路径已有文件会被覆盖。
        args.output.parent.mkdir(parents=True, exist_ok=True)
        torch.save(batch, args.output)
        print(f"Saved batch: {args.output.resolve()}")


if __name__ == "__main__":
    main()
