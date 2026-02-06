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


def parse_size_to_mb(size_str):
    """Converts human readable size string (e.g. '100 MiB', '2.5 GiB') to MB (float)."""
    if not size_str or size_str.strip() == "":
        return 0.0

    # Clean string
    size_str = size_str.strip()

    # Split value and unit
    parts = size_str.split()

    # Handle case with no unit (assume Bytes) or parse failure
    if len(parts) < 2:
        try:
            # Assume bytes if just a number
            return float(parts[0]) / (1024 * 1024)
        except ValueError:
            return 0.0

    try:
        val = float(parts[0])
    except ValueError:
        return 0.0

    unit = parts[1]

    # Constants for conversion to MB (using binary prefixes 1024 based on typical system outputs)
    factors = {
        "B": 1.0 / (1024 * 1024),
        "KiB": 1.0 / 1024,
        "KB": 1.0 / 1024,
        "MiB": 1.0,
        "MB": 1.0,
        "GiB": 1024.0,
        "GB": 1024.0,
        "TiB": 1024.0 * 1024.0,
        "TB": 1024.0 * 1024.0,
    }

    # Default to 0 if unit unknown, or assume MB?
    # For safety, strict match or return 0
    return val * factors.get(unit, 0.0)


def parse_and_plot(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found")
        return

    # We parse the file by splitting into blocks based on "write batch"
    # This is more robust than a single regex if lines are optional or reordered
    batches_data = []

    # Regex lookahead to split by "write batch <number>"
    blocks = re.split(r"(?=write batch \d+)", text)

    for block in blocks:
        if not block.strip().startswith("write batch"):
            continue

        data = {}

        # Batch Num
        m_batch = re.search(r"write batch (\d+)", block)
        if m_batch:
            data["Batch"] = int(m_batch.group(1))
        else:
            continue

        # Helper to extract size string for a given label
        def extract_size(label):
            # Matches "Label: 123.45 Unit"
            # Handles variations in whitespace
            m = re.search(rf"{label}:\s*(.+?)(?:\n|$)", block)
            return parse_size_to_mb(m.group(1)) if m else 0.0

        # Extract Metrics
        data["Pool_Reserve_MB"] = extract_size("pool reserve")
        data["Allocated_MB"] = extract_size("Allocated")

        # Typo compatibility for "Resident"
        res_mb = extract_size("Rsident")
        if res_mb == 0:
            res_mb = extract_size("Resident")
        data["Resident_MB"] = res_mb

        data["Active_MB"] = extract_size("Active")

        # Spill size (Optional, check multiple possible labels)
        spill = extract_size("Spill Disk Usage")
        if spill == 0:
            spill = extract_size("spill size")
        data["Spill_Disk_MB"] = spill

        # Cost
        m_cost = re.search(r"cost:\s*(\d+)\s*ms", block)
        data["Cost_ms"] = int(m_cost.group(1)) if m_cost else 0

        batches_data.append(data)

    if not batches_data:
        print("No matching log data found. Ensure logs contain 'write batch ...'")
        return

    # Create DataFrame
    df = pd.DataFrame(batches_data)
    df.sort_values("Batch", inplace=True)

    # Setup Plot
    fig, ax1 = plt.subplots(figsize=(14, 8))

    # X Axis
    ax1.set_xlabel("Batch Number")
    ax1.set_ylabel("Memory (MB)", color="tab:blue")

    # --- Plotting Memory Metrics ---

    # 1. Allocated (Blue Solid)
    ax1.plot(
        df["Batch"],
        df["Allocated_MB"],
        label="Jemalloc Allocated",
        color="tab:blue",
        linewidth=1.5,
        alpha=0.8,
    )

    # 2. Resident (Red Dashed)
    ax1.plot(
        df["Batch"],
        df["Resident_MB"],
        label="RSS (Resident)",
        color="tab:red",
        linestyle="--",
        linewidth=1.5,
        alpha=0.8,
    )

    # 3. Active (Green Dotted)
    ax1.plot(
        df["Batch"],
        df["Active_MB"],
        label="Jemalloc Active",
        color="tab:green",
        linestyle=":",
        linewidth=1.5,
        alpha=0.6,
    )

    # 4. Pool Reserve (Black Solid - The Limit)
    ax1.plot(
        df["Batch"],
        df["Pool_Reserve_MB"],
        label="DataFusion Pool Reserved",
        color="black",
        linewidth=2,
        linestyle="-",
        alpha=0.9,
    )

    # 5. Spill Size (Grey Filled Area)
    if "Spill_Disk_MB" in df.columns and df["Spill_Disk_MB"].max() > 0:
        ax1.fill_between(
            df["Batch"],
            df["Spill_Disk_MB"],
            label="Spill Disk Usage",
            color="grey",
            alpha=0.3,
        )
        # Optional: Add a line on top of the area
        ax1.plot(df["Batch"], df["Spill_Disk_MB"], color="grey", linewidth=1, alpha=0.5)

    ax1.tick_params(axis="y", labelcolor="tab:blue")
    ax1.grid(True, which="both", linestyle="--", alpha=0.5)

    # Combine legends from ax1
    lines, labels = ax1.get_legend_handles_labels()

    # --- Plotting Cost (Secondary Y Axis) ---
    ax2 = ax1.twinx()
    ax2.set_ylabel("Write Cost (ms)", color="tab:orange")
    scatter = ax2.scatter(
        df["Batch"],
        df["Cost_ms"],
        color="tab:orange",
        s=15,
        alpha=0.5,
        label="Batch Cost (ms)",
        zorder=0,
    )
    ax2.tick_params(axis="y", labelcolor="tab:orange")

    # Add scatter to legend list
    lines.append(scatter)
    labels.append("Batch Cost (ms)")

    # Display Legend
    ax1.legend(lines, labels, loc="upper left")

    plt.title(f"LakeSoul Writer Profiling: {file_path}")
    fig.tight_layout()

    # Save
    output_png = "memory_profiling.png"
    plt.savefig(output_png)
    print(f"Analysis complete! Plot saved to: {output_png}")
    # plt.show()


if __name__ == "__main__":
    input_file = sys.argv[1] if len(sys.argv) > 1 else "output.log"
    parse_and_plot(input_file)
