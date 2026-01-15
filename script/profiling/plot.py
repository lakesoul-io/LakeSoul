# /// script
# dependencies = [
#   "matplotlib",
#   "pandas",
# ]
# ///

import re
import sys

import matplotlib.pyplot as plt
import pandas as pd


def parse_and_plot(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        print(f"错误：找不到文件 '{file_path}'")
        return

    # 匹配 Rust 输出格式（兼容代码中的拼写错误 Rsident 和 Atctive）
    pattern = r"write batch (\d+) cost: (\d+) ms\nAllocated: (\d+) MB\nRsident: (\d+) MB\nActive: (\d+) MB"
    matches = re.findall(pattern, text)

    if not matches:
        print(
            "未发现匹配的日志数据。请确保日志包含 'write batch ... Allocated: ...' 等内容。"
        )
        return

    # 转换为 DataFrame 并转换数据类型
    df = pd.DataFrame(
        matches,
        columns=["Batch", "Cost_ms", "Allocated_MB", "Resident_MB", "Active_MB"],
    )
    df = df.astype(int)

    # 创建画布
    fig, ax1 = plt.subplots(figsize=(12, 7))

    # 绘制内存曲线 (左侧 Y 轴)
    ax1.set_xlabel("Batch Number")
    ax1.set_ylabel("Memory (MB)", color="tab:blue")
    ax1.plot(
        df["Batch"],
        df["Allocated_MB"],
        label="Allocated",
        color="tab:blue",
        linewidth=2,
    )
    ax1.plot(
        df["Batch"],
        df["Resident_MB"],
        label="Resident",
        color="tab:red",
        linestyle="--",
        alpha=0.7,
    )
    ax1.plot(
        df["Batch"],
        df["Active_MB"],
        label="Active",
        color="tab:green",
        linestyle=":",
        alpha=0.7,
    )
    ax1.tick_params(axis="y", labelcolor="tab:blue")
    ax1.grid(True, which="both", linestyle="--", alpha=0.5)
    ax1.legend(loc="upper left")

    # 绘制耗时曲线 (右侧 Y 轴，可选)
    ax2 = ax1.twinx()
    ax2.set_ylabel("Write Cost (ms)", color="tab:orange")
    ax2.scatter(
        df["Batch"],
        df["Cost_ms"],
        color="tab:orange",
        s=5,
        alpha=0.3,
        label="Batch Cost (ms)",
    )
    ax2.tick_params(axis="y", labelcolor="tab:orange")

    plt.title(f"LakeSoul Writer Profiling: {file_path}")
    fig.tight_layout()

    # 保存图片
    output_png = "memory_profiling.png"
    plt.savefig(output_png)
    print(f"分析完成！图片已保存至: {output_png}")
    plt.show()


if __name__ == "__main__":
    # 如果命令行没有传文件名，默认尝试读取 'output.log'
    input_file = sys.argv[1] if len(sys.argv) > 1 else "output.log"
    parse_and_plot(input_file)
