from collections.abc import Mapping
from typing import TypedDict

class TextIndexConfig(TypedDict):
    """A parsed entry of the ``text_index_columns`` table property."""

    column: str
    tokenizer: str
    with_positions: bool
    stored: bool
    rebuild_mode: str
    max_delta_ratio: float

def parse_text_index_configs(value: str) -> list[TextIndexConfig]: ...
def text_supported_tokenizers() -> list[str]: ...
def text_index_stats(
    file_paths: list[str],
    text_column: str,
) -> tuple[int, int, int, int] | None: ...
def build_shard_text_index(
    store_config: Mapping[str, str],
    file_paths: list[str],
    pk_column: str,
    text_column: str,
    tokenizer: str = "jieba",
    with_positions: bool = True,
    stored: bool = False,
) -> str: ...
def rebuild_shard_text_index(
    store_config: Mapping[str, str],
    file_paths: list[str],
    pk_column: str,
    text_column: str,
    tokenizer: str = "jieba",
    with_positions: bool = True,
    stored: bool = False,
) -> str: ...
