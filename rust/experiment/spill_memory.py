import re

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def parse_spill_file(filename):
    """解析spill.out文件并返回长表格式数据"""
    data = []
    # 修正后的正则：匹配非括号、非空格的所有字符作为名字
    # 捕获组 1: 完整名字, 2: 是否可溢写, 3: 内存值
    pattern = re.compile(r"([^ \t,]+?)\(can spill: (true|false)\)\[([\d.]+)\]")

    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split(" ", 1)
            if len(parts) != 2:
                continue

            timestamp = float(parts[0])
            operators_str = parts[1]

            # 使用 findall 直接抓取这一行所有的算子信息
            matches = pattern.findall(operators_str)

            for name, can_spill, memory in matches:
                data.append(
                    {
                        "time": timestamp,
                        "consumer": f"{name}(can spill:{can_spill})",
                        "memory": int(float(memory)),
                    }
                )

    df = pd.DataFrame(data)
    return df


# ================= 数据处理 =================
df = parse_spill_file("/data/jiax_space/LakeSoul/rust/lakesoul-io/spill.out")

sns.set_theme(style="white")
sns.set_palette("deep")  # 或者用 "bright"、"dark"

plt.figure(figsize=(30, 8))
plt.rcParams["svg.fonttype"] = "none"  # 建议同时保留文字为路径或字体
plt.rcParams["savefig.bbox"] = "tight"

sns.lineplot(
    data=df,
    x="time",
    y="memory",
    hue="consumer",
    linewidth=0.8,
    markers=False,
)

plt.xlabel("time (ms)", fontsize=12, fontweight="bold")
plt.ylabel("memory (KB)", fontsize=12, fontweight="bold")
plt.savefig("test.svg", dpi=300)
