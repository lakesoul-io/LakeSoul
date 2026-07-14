from collections.abc import Mapping


def build_shard_vector_index(
    store_config: dict[str, str],
    file_paths: list[str],
    pk_column: str,
    vector_column: str,
    dim: int,
    nlist: int = 256,
    total_bits: int = 7,
    metric: str = "L2",
) -> str: ...
