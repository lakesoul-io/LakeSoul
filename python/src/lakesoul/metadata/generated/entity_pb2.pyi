from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CommitOp(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CompactionCommit: _ClassVar[CommitOp]
    AppendCommit: _ClassVar[CommitOp]
    MergeCommit: _ClassVar[CommitOp]
    UpdateCommit: _ClassVar[CommitOp]
    DeleteCommit: _ClassVar[CommitOp]

class FileOp(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    add: _ClassVar[FileOp]
    del: _ClassVar[FileOp]
CompactionCommit: CommitOp
AppendCommit: CommitOp
MergeCommit: CommitOp
UpdateCommit: CommitOp
DeleteCommit: CommitOp
add: FileOp
del: FileOp

class MetaInfo(_message.Message):
    __slots__ = ("list_partition", "table_info", "read_partition_info")
    LIST_PARTITION_FIELD_NUMBER: _ClassVar[int]
    TABLE_INFO_FIELD_NUMBER: _ClassVar[int]
    READ_PARTITION_INFO_FIELD_NUMBER: _ClassVar[int]
    list_partition: _containers.RepeatedCompositeFieldContainer[PartitionInfo]
    table_info: TableInfo
    read_partition_info: _containers.RepeatedCompositeFieldContainer[PartitionInfo]
    def __init__(self, list_partition: _Optional[_Iterable[_Union[PartitionInfo, _Mapping]]] = ..., table_info: _Optional[_Union[TableInfo, _Mapping]] = ..., read_partition_info: _Optional[_Iterable[_Union[PartitionInfo, _Mapping]]] = ...) -> None: ...

class TableInfo(_message.Message):
    __slots__ = ("table_id", "table_namespace", "table_name", "table_path", "table_schema", "table_schema_arrow_ipc", "table_schema_arrow_ipc_json_hash", "properties", "partitions", "domain")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_PATH_FIELD_NUMBER: _ClassVar[int]
    TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_SCHEMA_ARROW_IPC_FIELD_NUMBER: _ClassVar[int]
    TABLE_SCHEMA_ARROW_IPC_JSON_HASH_FIELD_NUMBER: _ClassVar[int]
    PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    table_id: str
    table_namespace: str
    table_name: str
    table_path: str
    table_schema: str
    table_schema_arrow_ipc: bytes
    table_schema_arrow_ipc_json_hash: str
    properties: str
    partitions: str
    domain: str
    def __init__(self, table_id: _Optional[str] = ..., table_namespace: _Optional[str] = ..., table_name: _Optional[str] = ..., table_path: _Optional[str] = ..., table_schema: _Optional[str] = ..., table_schema_arrow_ipc: _Optional[bytes] = ..., table_schema_arrow_ipc_json_hash: _Optional[str] = ..., properties: _Optional[str] = ..., partitions: _Optional[str] = ..., domain: _Optional[str] = ...) -> None: ...

class PartitionInfo(_message.Message):
    __slots__ = ("table_id", "partition_desc", "version", "commit_op", "timestamp", "snapshot", "expression", "domain", "pinned")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_DESC_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    COMMIT_OP_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    PINNED_FIELD_NUMBER: _ClassVar[int]
    table_id: str
    partition_desc: str
    version: int
    commit_op: CommitOp
    timestamp: int
    snapshot: _containers.RepeatedCompositeFieldContainer[Uuid]
    expression: str
    domain: str
    pinned: bool
    def __init__(self, table_id: _Optional[str] = ..., partition_desc: _Optional[str] = ..., version: _Optional[int] = ..., commit_op: _Optional[_Union[CommitOp, str]] = ..., timestamp: _Optional[int] = ..., snapshot: _Optional[_Iterable[_Union[Uuid, _Mapping]]] = ..., expression: _Optional[str] = ..., domain: _Optional[str] = ..., pinned: bool = ...) -> None: ...

class Namespace(_message.Message):
    __slots__ = ("namespace", "properties", "comment", "domain")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    properties: str
    comment: str
    domain: str
    def __init__(self, namespace: _Optional[str] = ..., properties: _Optional[str] = ..., comment: _Optional[str] = ..., domain: _Optional[str] = ...) -> None: ...

class DataFileOp(_message.Message):
    __slots__ = ("path", "file_op", "size", "file_exist_cols")
    PATH_FIELD_NUMBER: _ClassVar[int]
    FILE_OP_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    FILE_EXIST_COLS_FIELD_NUMBER: _ClassVar[int]
    path: str
    file_op: FileOp
    size: int
    file_exist_cols: str
    def __init__(self, path: _Optional[str] = ..., file_op: _Optional[_Union[FileOp, str]] = ..., size: _Optional[int] = ..., file_exist_cols: _Optional[str] = ...) -> None: ...

class DataCommitInfo(_message.Message):
    __slots__ = ("table_id", "partition_desc", "commit_id", "file_ops", "commit_op", "timestamp", "committed", "domain", "pinned")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_DESC_FIELD_NUMBER: _ClassVar[int]
    COMMIT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_OPS_FIELD_NUMBER: _ClassVar[int]
    COMMIT_OP_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    PINNED_FIELD_NUMBER: _ClassVar[int]
    table_id: str
    partition_desc: str
    commit_id: Uuid
    file_ops: _containers.RepeatedCompositeFieldContainer[DataFileOp]
    commit_op: CommitOp
    timestamp: int
    committed: bool
    domain: str
    pinned: bool
    def __init__(self, table_id: _Optional[str] = ..., partition_desc: _Optional[str] = ..., commit_id: _Optional[_Union[Uuid, _Mapping]] = ..., file_ops: _Optional[_Iterable[_Union[DataFileOp, _Mapping]]] = ..., commit_op: _Optional[_Union[CommitOp, str]] = ..., timestamp: _Optional[int] = ..., committed: bool = ..., domain: _Optional[str] = ..., pinned: bool = ...) -> None: ...

class TableNameId(_message.Message):
    __slots__ = ("table_name", "tableId", "table_namespace", "domain")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLEID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    tableId: str
    table_namespace: str
    domain: str
    def __init__(self, table_name: _Optional[str] = ..., tableId: _Optional[str] = ..., table_namespace: _Optional[str] = ..., domain: _Optional[str] = ...) -> None: ...

class DiscardCompressedFileInfo(_message.Message):
    __slots__ = ("file_path", "table_path", "partition_desc", "timestamp", "t_date")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    TABLE_PATH_FIELD_NUMBER: _ClassVar[int]
    PARTITION_DESC_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    T_DATE_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    table_path: str
    partition_desc: str
    timestamp: int
    t_date: str
    def __init__(self, file_path: _Optional[str] = ..., table_path: _Optional[str] = ..., partition_desc: _Optional[str] = ..., timestamp: _Optional[int] = ..., t_date: _Optional[str] = ...) -> None: ...

class TablePathId(_message.Message):
    __slots__ = ("table_path", "table_id", "table_namespace", "domain")
    TABLE_PATH_FIELD_NUMBER: _ClassVar[int]
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    table_path: str
    table_id: str
    table_namespace: str
    domain: str
    def __init__(self, table_path: _Optional[str] = ..., table_id: _Optional[str] = ..., table_namespace: _Optional[str] = ..., domain: _Optional[str] = ...) -> None: ...

class Uuid(_message.Message):
    __slots__ = ("high", "low")
    HIGH_FIELD_NUMBER: _ClassVar[int]
    LOW_FIELD_NUMBER: _ClassVar[int]
    high: int
    low: int
    def __init__(self, high: _Optional[int] = ..., low: _Optional[int] = ...) -> None: ...

class SnapshotInfo(_message.Message):
    __slots__ = ("table_id", "snapshot_id", "created_at", "description")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    table_id: str
    snapshot_id: int
    created_at: int
    description: str
    def __init__(self, table_id: _Optional[str] = ..., snapshot_id: _Optional[int] = ..., created_at: _Optional[int] = ..., description: _Optional[str] = ...) -> None: ...

class SnapshotCommitInfo(_message.Message):
    __slots__ = ("table_id", "snapshot_id", "partition_desc", "version", "commit_id")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_DESC_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    COMMIT_ID_FIELD_NUMBER: _ClassVar[int]
    table_id: str
    snapshot_id: int
    partition_desc: str
    version: int
    commit_id: Uuid
    def __init__(self, table_id: _Optional[str] = ..., snapshot_id: _Optional[int] = ..., partition_desc: _Optional[str] = ..., version: _Optional[int] = ..., commit_id: _Optional[_Union[Uuid, _Mapping]] = ...) -> None: ...

class SnapshotTagInfo(_message.Message):
    __slots__ = ("table_id", "tag", "snapshot_id", "created_at", "expire_at")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRE_AT_FIELD_NUMBER: _ClassVar[int]
    table_id: str
    tag: str
    snapshot_id: int
    created_at: int
    expire_at: int
    def __init__(self, table_id: _Optional[str] = ..., tag: _Optional[str] = ..., snapshot_id: _Optional[int] = ..., created_at: _Optional[int] = ..., expire_at: _Optional[int] = ...) -> None: ...

class JniWrapper(_message.Message):
    __slots__ = ("namespace", "table_info", "table_path_id", "table_name_id", "partition_info", "data_commit_info", "discard_compressed_file_info", "snapshot_info", "snapshot_commit_info", "snapshot_tag_info")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    TABLE_INFO_FIELD_NUMBER: _ClassVar[int]
    TABLE_PATH_ID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_INFO_FIELD_NUMBER: _ClassVar[int]
    DATA_COMMIT_INFO_FIELD_NUMBER: _ClassVar[int]
    DISCARD_COMPRESSED_FILE_INFO_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_INFO_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_COMMIT_INFO_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_TAG_INFO_FIELD_NUMBER: _ClassVar[int]
    namespace: _containers.RepeatedCompositeFieldContainer[Namespace]
    table_info: _containers.RepeatedCompositeFieldContainer[TableInfo]
    table_path_id: _containers.RepeatedCompositeFieldContainer[TablePathId]
    table_name_id: _containers.RepeatedCompositeFieldContainer[TableNameId]
    partition_info: _containers.RepeatedCompositeFieldContainer[PartitionInfo]
    data_commit_info: _containers.RepeatedCompositeFieldContainer[DataCommitInfo]
    discard_compressed_file_info: _containers.RepeatedCompositeFieldContainer[DiscardCompressedFileInfo]
    snapshot_info: _containers.RepeatedCompositeFieldContainer[SnapshotInfo]
    snapshot_commit_info: _containers.RepeatedCompositeFieldContainer[SnapshotCommitInfo]
    snapshot_tag_info: _containers.RepeatedCompositeFieldContainer[SnapshotTagInfo]
    def __init__(self, namespace: _Optional[_Iterable[_Union[Namespace, _Mapping]]] = ..., table_info: _Optional[_Iterable[_Union[TableInfo, _Mapping]]] = ..., table_path_id: _Optional[_Iterable[_Union[TablePathId, _Mapping]]] = ..., table_name_id: _Optional[_Iterable[_Union[TableNameId, _Mapping]]] = ..., partition_info: _Optional[_Iterable[_Union[PartitionInfo, _Mapping]]] = ..., data_commit_info: _Optional[_Iterable[_Union[DataCommitInfo, _Mapping]]] = ..., discard_compressed_file_info: _Optional[_Iterable[_Union[DiscardCompressedFileInfo, _Mapping]]] = ..., snapshot_info: _Optional[_Iterable[_Union[SnapshotInfo, _Mapping]]] = ..., snapshot_commit_info: _Optional[_Iterable[_Union[SnapshotCommitInfo, _Mapping]]] = ..., snapshot_tag_info: _Optional[_Iterable[_Union[SnapshotTagInfo, _Mapping]]] = ...) -> None: ...
