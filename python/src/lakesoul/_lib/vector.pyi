from collections.abc import Mapping
from typing import TypedDict

class VectorIndexConfig(TypedDict):
    """A parsed entry of the ``vector_index_columns`` table property."""

    column: str
    dim: int
    nlist: int
    total_bits: int
    metric: str
    rotator_type: str
    seed: int
    use_faster_config: bool

def parse_vector_index_configs(value: str) -> list[VectorIndexConfig]: ...
def build_shard_vector_index(
    store_config: Mapping[str, str],
    file_paths: list[str],
    pk_column: str,
    vector_column: str,
    dim: int,
    nlist: int = 256,
    total_bits: int = 7,
    metric: str = "L2",
    rotator_type: str = "FhtKac",
    seed: int = 42,
    use_faster_config: bool = True,
) -> str: ...
