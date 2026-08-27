from decimal import Decimal

import pyarrow as pa

from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
base_uri = "s3://lakeinsight-bucket/lakeinsight-v1/warehouse/default"
table_name = "ipc_mixed_formats"
table_path = f"{base_uri}/{table_name}"

catalog.drop_table(table_name, namespace="default", if_exists=True)

mixed_schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("amount", pa.decimal128(10, 2)),
        pa.field("source_format", pa.string()),
        pa.field("dt", pa.string()),
    ],
    metadata={"schema_source": "mixed-format-e2e"},
)

table = catalog.create_table(
    table_name,
    namespace="default",
    path=table_path,
    schema=mixed_schema,
    partition_by=["dt"],
)

formats = ("parquet", "vortex", "vortex-compact")

for index, physical_format in enumerate(formats, start=1):
    batch = pa.table(
        {
            "id": pa.array([index], type=pa.int64()),
            "amount": pa.array(
                [Decimal(f"{index * 10}.00")],
                type=pa.decimal128(10, 2),
            ),
            "source_format": pa.array(
                [physical_format],
                type=pa.string(),
            ),
            "dt": pa.array(
                ["2026-08-25"],
                type=pa.string(),
            ),
        },
        schema=mixed_schema,
    )

    result = table.write_arrow(batch, format=physical_format)

    assert result.row_count == 1
    assert result.files

    for file_info in result.files:
        print(file_info)

        actual_format = file_info.other_info.get("physical_format")

        if physical_format == "parquet":
            # Parquet 当前通过文件扩展名识别。
            # other_info 通常只有 num_row_groups，没有 physical_format。
            assert file_info.path.endswith(".parquet"), file_info.path
            assert actual_format in (None, "parquet"), file_info.other_info
            assert int(file_info.other_info["num_row_groups"]) >= 1

        else:
            # vortex 和 vortex-compact 后缀都是 .vortex，
            # 必须依靠 physical_format 区分。
            assert file_info.path.endswith(".vortex"), file_info.path
            assert actual_format == physical_format, (
                f"requested={physical_format}, "
                f"actual={actual_format}, "
                f"other_info={dict(file_info.other_info)}"
            )

        print(
            "path=",
            file_info.path,
            "requested_format=",
            physical_format,
            "reported_format=",
            actual_format,
            "other_info=",
            dict(file_info.other_info),
        )

output = (
    catalog.table(table_name, namespace="default").scan().to_arrow_table().sort_by("id")
)

print(output)

assert output.num_rows == 3
assert output.column("source_format").to_pylist() == [
    "parquet",
    "vortex",
    "vortex-compact",
]

print("mixed format: PASS")
