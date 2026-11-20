# PostgreSQL Wire Type Coverage Plan

- Status: implemented (2026-11-20)
- Branch: `feature/pg-wire-type-support`
- Base: `upstream/main` @ `aebeaba24` (PostgreSQL wire front end)
- Scope: DDL, storage schema, INSERT/UPDATE/UPSERT, COPY, wire results and
  parameters, `pg_catalog` introspection, and the metadata used by the
  PostgreSQL subscriber.

## Implementation status (2026-11-20)

The first implementation follows the Arrow-first scope, with these concrete
decisions after validating DataFusion and the storage writers:

- `uuid` is stored as canonical UTF-8 with `pg.type=uuid`. DataFusion can
  plan string literals/parameters for it, while Arrow's canonical
  `FixedSizeBinary(16)` path cannot be produced without a custom cast. The
  wire layer still reports the UUID OID and converts to/from 16 raw bytes for
  binary clients.
- `time(p)` and `timestamp(p)`/`timestamptz(p)` support the Arrow units
  `p = 0/3/6`; other precisions remain `0A000`. Fractions beyond the selected
  unit are truncated by the current cast path (documented deviation from
  PostgreSQL's rounding).
- `numeric(p,s)` up to 38 uses `Decimal128`; 39..76 uses `Decimal256`.
  Precision above 76 and bare `numeric` remain refused.
- `money` is stored as `Decimal128(19,2)` with a `pg.type=money` marker and
  reports the MONEY OID.
- `varchar(n)`/`char(n)` keep their length in `pg.typmod`, enforce it on
  INSERT/COPY, and report `character varying(n)`/`character(n)`. Exact
  `char(n)` blank-padding semantics are not emulated.
- One-dimensional arrays use Arrow `List`; array primary keys and partition
  columns are refused. An INSERT `VALUES` plan is rewritten so the `Values`
  node runs against physical list types and a projection casts each column
  back to the declared field: DataFusion's values executor rebuilds lists from
  `ScalarValue`s and drops element field metadata. Array literals are checked
  element by element for INSERT, UPDATE, UPSERT and COPY.
- `interval` and `inet` columns are **not** supported: Parquet and Vortex
  cannot persist Arrow intervals, and Arrow has no inet type. Interval
  expressions continue to work. This is narrower than the original plan and
  follows the Arrow/storage-support rule.
- `pg.type`/`pg.typmod`/`pg.array` are implemented in the
  `datafusion-postgres` fork (`feature/pg-type-coverage`, PR #1) together with
  UUID/money/Decimal256 encoders, array OIDs, `pg_attribute` typmod/ndims and
  parameter descriptions. The LakeSoul dependency is pinned to that branch
  commit.

Where the original design below disagrees with this status (for example the
UUID storage type or interval support), the implementation status wins.

## 1. Goal and non-goals

### Goal

Close the most important PostgreSQL type gaps in `postgres-lakesoul` without
trying to reproduce every PostgreSQL type. A type is in scope when it has a
natural Arrow/DataFusion representation, or when a small metadata layer can
carry the PostgreSQL semantics over an Arrow-native physical type.

The requested gaps are:

- `uuid`
- array columns
- `time`, `interval`, `inet`, `money`
- `char(n)` / `varchar(n)` length
- `numeric` precision above 38
- `timestamp(p)` / `timestamptz(p)` with precision other than 6

### Non-goals

These are deliberately out of scope unless a concrete consumer requires them:

- `numeric` with no precision/scale, or precision above Arrow `Decimal256`'s
  76 digits.
- `timestamp(p)` / `time(p)` precisions that Arrow cannot represent as a time
  unit (1, 2, 4, 5). Arrow has second, millisecond, microsecond and nanosecond
  units; PostgreSQL allows 0-6.
- `time with time zone` (`timetz`), `cidr`, `macaddr`, `bit`/`varbit`, `xml`,
  ranges, enums and composite types.
- Multi-dimensional arrays and PostgreSQL array lower-bound metadata.
- Exact `char(n)` blank-padding and trailing-space comparison semantics.
- Full PostgreSQL interval field/precision grammar.
- `inet` as a first-class type: Arrow has no inet type. A text-backed
  compatibility mode is a possible follow-up, not part of the first cut.

The guiding rule is: use Arrow/DataFusion capabilities first; do not invent a
large custom type system just to match PostgreSQL byte-for-byte.

## 2. Current state and evidence

### DDL

`rust/postgres-lakesoul/src/ddl.rs::arrow_type` maps a small set of SQL types
and refuses everything else with `0A000`. Today it maps:

- integers, floats, boolean, text/varchar/char (all to `Utf8`), date,
  timestamp/timestamptz (microseconds), bytea, `decimal(p,s)` with `p <= 38`.

It explicitly refuses `UUID`, arrays, `TIME`, `INTERVAL`, `INET`, `MONEY`,
`NUMERIC` above 38, and `TIMESTAMP(p)` for `p != 6`. `ALTER TABLE ... ADD
COLUMN` and `ALTER COLUMN ... TYPE` use the same function.

`varchar(n)` and `char(n)` are accepted but the length is discarded. The
catalog therefore reports `text`; inserts longer than `n` succeed.

### Field metadata

The project already uses field metadata as a semantic layer:

- `pg.json` distinguishes `json` from `jsonb`;
- `pg.char` marks PostgreSQL's internal `"char"`;
- `pg.vector` marks pgvector columns;
- `pg.oid_alias` marks oid/reg* columns.

`arrow-pg` (`datafusion-postgres` fork) uses these markers in
`field_into_pg_type`, `pg_attribute`, parameter overrides and encoders.
`lakesoul_common::ser::arrow_java` preserves field metadata in both
`table_schema` and the Arrow IPC mirror.

### Wire layer

`datafusion-postgres/arrow-pg` maps physical Arrow types to PG OIDs. It already
handles `Time32`, `Time64`, `Interval`, `List` and several array element types,
but:

- scalar `Decimal256` is not encoded (`pg_numeric.rs` rejects it explicitly);
- `UUID`, `INET`, `MONEY`, `BPCHAR` and `VARCHAR` have no semantic mapping;
- `pg_attribute` hardcodes `atttypmod = -1` and `attndims = 0`, so
  `varchar(n)`, `numeric(p,s)` and `timestamp(p)` are not visible to clients;
- parameter decoding in `arrow-pg/src/datatypes/df.rs` covers only part of the
  array type set and has no `Decimal256` path;
- `to_regtype` in `pg_compat.rs` does not know `inet`, `money`, `bpchar` or
  array names.

### DataFusion planner limitations

Observed on the current branch against a local server:

```text
CREATE TABLE t (v UUID)            -> 0A000
CREATE TABLE t (v INT[])           -> 0A000
CREATE TABLE t (v TIME)            -> 0A000
CREATE TABLE t (v INTERVAL)        -> 0A000
CREATE TABLE t (v INET)            -> 0A000
CREATE TABLE t (v MONEY)           -> 0A000
CREATE TABLE t (v NUMERIC(40,2))   -> 0A000
CREATE TABLE t (v TIMESTAMP(3))    -> 0A000
```

The engine already evaluates some of these in expressions:

```text
SELECT ARRAY[1,2]                  -> {1,2}
SELECT TIME '12:34:56'             -> 12:34:56.000000
SELECT INTERVAL '1 day'            -> 1 day
```

But DataFusion itself rejects `UUID`, `INET`, `MONEY`, `TIME(p)` and
`TIMESTAMP(p)` for `p` outside `{0,3,6}`, and rejects interval qualifiers such
as `INTERVAL '1 day 02:03:04'`. We will not work around every DataFusion
parser limitation.

### COPY and subscription

- `copy_in.rs` decodes only a fixed scalar set. It has no `Time32/64`,
  `Interval`, `List` or `Decimal256` support, and does not validate
  `varchar(n)`/`char(n)`.
- `copy_out.rs` formats only the same scalar set; it has no time, interval,
  array or `Decimal256` output.
- `subscribe/source.rs` maps `varchar`/`char` to `Text` and has no
  interval/inet/money/array source types. This must stay consistent with the
  new target metadata, at least for the types the subscriber accepts.

## 3. Design principles

1. **Physical Arrow type plus field metadata is the schema.** Do not encode
   PostgreSQL semantics only in the client-visible OID; every writer and reader
   must be able to recover the semantic type from the stored schema.
2. **Prefer Arrow-native representations.** Use `FixedSizeBinary(16)` +
   `arrow.uuid` for UUID, `Time32/64` for time, `Interval(MonthDayNano)` for
   intervals, `List` for arrays, `Decimal128/256` for numerics, and
   `Timestamp(Second|Millisecond|Microsecond)` for timestamp precision.
3. **No metadata migration.** New markers are additive field metadata inside the
   existing Arrow schema. Existing tables keep working; the new behavior applies
   to newly created/altered columns.
4. **Explicit refusal over silent coercion.** Unsupported precision, array
   dimensions, interval qualifiers and types are refused with `0A000` and a
   clear message, not stored as a different type.
5. **Text protocol first, binary where cheap.** Every in-scope type must work in
   the PostgreSQL text protocol. Binary protocol support is implemented where
   the Arrow representation maps directly (UUID raw 16 bytes, money int64
   cents, numeric, time, interval, arrays) and documented otherwise.
6. **Cross-engine readability.** Table schemas must remain readable by
   Spark/Flink. `arrow.uuid` is an Arrow canonical extension and
   `FixedSizeBinary(16)` has a Spark binary fallback; field metadata is
   preserved but not required by other engines.

## 4. Proposed support matrix

| PostgreSQL type | Physical Arrow storage | Field metadata | Wire type | Notes |
| --- | --- | --- | --- | --- |
| `uuid` | `FixedSizeBinary(16)` | `arrow.uuid` extension + `pg.type=uuid` | `UUID` | Canonical lowercase hyphenated text on text protocol; raw 16 bytes on binary |
| one-dimensional array | `List<element>` | inner field metadata; `pg.array=1` | `ELEMENT[]` | Element must be an in-scope type; reject multi-dimensional and array PK/partition |
| `time(p)` | `Time32(Second)` for `p=0`, `Time32(Millisecond)` for `p=3`, `Time64(Microsecond)` for `p=6`/default | `pg.type=time`, `pg.typmod=p` | `TIME` | Reject `p` 1,2,4,5 (no Arrow unit); `timetz` rejected |
| `interval` | `Interval(MonthDayNano)` | `pg.type=interval` | `INTERVAL` | Bare interval only; DataFusion-supported syntax only |
| `varchar(n)` | `Utf8` | `pg.type=varchar`, `pg.typmod=n+4` | `VARCHAR` | Length is a local constraint on Arrow Utf8, not a new physical type; enforce it and report typmod |
| `char(n)` | `Utf8` | `pg.type=bpchar`, `pg.typmod=n+4` | `BPCHAR` | Same Utf8 constraint; enforce length, no exact padding/comparison semantics in phase 1 |
| `numeric(p,s)`, `p<=38` | `Decimal128(p,s)` | `pg.type=numeric`, `pg.typmod=((p<<16)|s)+4` | `NUMERIC` | Existing behavior |
| `numeric(p,s)`, `39<=p<=76` | `Decimal256(p,s)` | same as above | `NUMERIC` | Reject `p>76`; bare numeric stays rejected |
| `timestamp(p)` | `Timestamp(Second)` for `p=0`, `Timestamp(Millisecond)` for `p=3`, `Timestamp(Microsecond)` for `p=6`/default | `pg.type=timestamp`, `pg.typmod=p` | `TIMESTAMP` | Reject 1,2,4,5; no timezone |
| `timestamptz(p)` | same units with UTC timezone | `pg.type=timestamptz`, `pg.typmod=p` | `TIMESTAMPTZ` | Same precision limits |
| `money` | `Decimal128(19,2)` | `pg.type=money` | `MONEY` | Decimal storage; wire text is `$...`, binary is int64 cents |
| `inet` | not in first cut | — | — | No Arrow-native type; optional text-backed follow-up |

`money` is the only emulated type in the first cut. It uses an Arrow-native
decimal so arithmetic and storage stay ordinary; only the wire adapter and
catalog marker are PostgreSQL-specific.

## 5. Architecture changes

### 5.1 A single type-metadata module

Add `rust/postgres-lakesoul/src/pg_types.rs` (or extend `ddl.rs` if the team
prefers fewer files) with:

- `PgTypeMetadata` / helpers:
  - `pg_type(field) -> Option<PgKind>`
  - `pg_typmod(field) -> Option<i32>`
  - `pg_is_array(field) -> bool`
  - `with_pg_type(field, kind, typmod) -> Field`
- `field_for_sql_type(name, sql_type, nullable) -> PgWireResult<Field>`:
  - converts SQL type to the physical Arrow type;
  - attaches extension metadata and `pg.type`/`pg.typmod`;
  - recursively converts array element types.
- `pg_input` validation helpers:
  - `parse_uuid`, `parse_money`, length validation, timestamp/time
    rounding, interval passthrough.
- `pg_output` formatting helpers where the generic Arrow encoder cannot produce
  PostgreSQL text (UUID, money, char padding).

Existing `pg.json` and `pg.char` handling stays where it is; the new module
should share the metadata-key constants with `arrow-pg` (the fork already
exports `PG_JSON_KEY`, `PG_CHAR_KEY`, etc.).

Marker contract:

| Key | Value | Use |
| --- | --- | --- |
| `pg.type` | `uuid`, `varchar`, `bpchar`, `time`, `interval`, `numeric`, `timestamp`, `timestamptz`, `money` | Semantic type when physical type is ambiguous |
| `pg.typmod` | PostgreSQL `atttypmod` as decimal text | Precision/length for catalog and format_type |
| `pg.array` | `1` | Marks a one-dimensional semantic array (inner field carries the element type) |
| `arrow.uuid` | Arrow canonical extension | UUID semantic extension on `FixedSizeBinary(16)` |

`pg.type`/`pg.typmod` must be stored in both `table_schema` and the Arrow IPC
mirror; no database migration is required.

### 5.2 DataFusion `TypePlanner`

DataFusion 55 exposes `TypePlanner::plan_type_field`, which is called before
the default SQL type conversion. Register a `PgTypePlanner` on the LakeSoul
session state:

- `UUID` -> `FixedSizeBinary(16)` + `arrow.uuid` + `pg.type=uuid`
- `TIME(p)` -> supported Arrow time unit + `pg.type=time` / typmod
- `TIMESTAMP(p)` / `TIMESTAMPTZ(p)` -> supported Arrow timestamp unit + marker
- `VARCHAR(n)` / `CHAR(n)` -> `Utf8` + marker/typmod
- `NUMERIC(p,s)` -> `Decimal128` or `Decimal256` + marker/typmod
- `INTERVAL` -> `Interval(MonthDayNano)` + marker
- `Array(inner)` -> `List<field_for_sql_type(inner)>`
- unsupported types return `None` so the normal `0A000` path remains

This covers casts and expressions such as `'...'::uuid`, `ARRAY[...]::uuid[]`
and `CAST(... AS NUMERIC(50, 2))` without teaching DataFusion a separate type
system. The same planner is used by the DDL hook, so DDL and expression
planning cannot disagree.

For casts that need validation (UUID, money), add a small parser rewrite or a
validating UDF (`pg_uuid(text)`, `pg_money(text)`) so `SELECT 'not-a-uuid'::uuid`
reports `22P02` instead of silently passing a string. Table writes perform the
same validation in the write path.

### 5.3 DDL and ALTER TABLE

- Replace `arrow_type(&SqlDataType) -> DataType` with
  `field_for_sql_type(name, &SqlDataType, nullable) -> Field`.
- `build_schema` and `ALTER TABLE ADD COLUMN` use the field-returning function.
- `ALTER TABLE ALTER COLUMN TYPE` must carry a full `Field`, not just a
  `DataType`, so markers and typmods survive.
- `lakesoul_common::schema_evolution::ColumnChange::Widen` must carry the
  target `Field` (or a `ColumnType` with metadata), and `can_widen` must compare
  metadata as well as physical types:
  - `varchar(n) -> varchar(m)` only when `m >= n`;
  - `char(n) -> char(m)` only when `m >= n`;
  - `numeric(p,s) -> numeric(p',s')` using the existing decimal rule, including
    `Decimal128 -> Decimal256`;
  - `timestamp(p) -> timestamp(q)` only when `q >= p` (or same unit);
  - `time(p) -> time(q)` only when `q >= p`;
  - UUID / interval / money are only equal to themselves.
- `check_evolution` must not treat `Utf8` varchar(10) and `Utf8` text as the
  same type. This is the most important schema-evolution change.

### 5.4 Wire layer (`datafusion-postgres` fork)

Bump the pinned `datafusion-postgres` revision after the following fork changes.
These are generic PostgreSQL protocol behavior and belong in the fork, not in
LakeSoul-specific code.

1. `arrow-pg/src/datatypes.rs`
   - `field_into_pg_type`:
     - `arrow.uuid` -> `Type::UUID`;
     - `pg.type=money` -> `Type::MONEY`;
     - `pg.type=varchar` -> `Type::VARCHAR`; `pg.type=bpchar` -> `Type::BPCHAR`;
     - `pg.type=time` / `interval` -> existing scalar mapping;
     - `Decimal256` -> `Type::NUMERIC`;
     - `List` -> the matching array OID using the inner field's semantic type,
       not only its physical type.
2. `arrow-pg/src/encoder.rs`
   - scalar `Decimal256` text and binary (base-10000 numeric payload);
   - UUID text (hyphenated) and binary (raw 16 bytes);
   - money text (`$`/locale formatting) and binary (int64 cents);
   - `varchar`/`bpchar` text and binary (UTF-8 bytes);
   - semantic array element encoding using the inner marker.
3. `arrow-pg/src/list_encoder.rs`
   - array cases for UUID, money, numeric/Decimal256, date/time/timestamp,
     binary and varchar/bpchar that are still missing;
   - element `FieldInfo` derived from the array OID.
4. `datafusion-pg-catalog/src/pg_catalog/pg_attribute.rs`
   - `atttypmod` from `pg.typmod` (currently hardcoded `-1`);
   - `attndims` = 1 for `List` (currently hardcoded 0);
   - type length/by-value/align/storage for UUID, money, inet, varchar/bpchar,
     time, interval, numeric;
   - type OID from the same semantic helper as `field_into_pg_type`.
5. `datafusion-pg-catalog/src/pg_catalog/format_type.rs`
   - complete OID/typmod formatting for the new types (most cases already exist;
     add money/inet and any missing array base types).
6. `arrow-pg/src/datatypes/df.rs`
   - parameter decoding for UUID (16 bytes or text -> `FixedSizeBinary(16)`),
     money (int64 cents -> `Decimal128(19,2)`), varchar/bpchar,
     `Decimal256`, and the missing array types;
   - use `Type::UUID`, `Type::MONEY`, `Type::VARCHAR`, `Type::BPCHAR` rather than
     generic `String`/`i64` decoders where the `FromSql` implementation does not
     accept the OID.
7. `datafusion-postgres/src/planner.rs`
   - `parameter_override_types` reads `pg.type`/`pg.typmod`/array metadata so
     prepared statements report UUID/VARCHAR/BPCHAR/MONEY/TIME/INTERVAL and
     array OIDs instead of `TEXT`/`INT8`/`UNKNOWN`.

If a fork bump is not acceptable for the first cut, the fallback is a local
response-rewrite layer similar to `pg_numeric.rs`: change `FieldInfo` types and
re-encode text cells. That is a temporary workaround, not the target design.

### 5.5 Input paths

#### INSERT / UPDATE / UPSERT

`insert.rs` currently delegates to DataFusion planning. Add a semantic target
check before the write:

- target `Field` carries `pg.type`; validate untyped string literals and
  parameters:
  - UUID: parse 16-byte UUID, canonicalize; invalid -> `22P02`;
  - varchar/char: character count <= n, trailing-space rule, overflow `22001`;
  - money: parse currency/decimal text; range `22003`;
  - timestamp/time: round to the target unit (if the target unit is
    second/millisecond/microsecond);
- `lakesoul-datafusion/src/pg_assignment.rs`:
  - add `Interval` to `is_temporal`;
  - add semantic target checks where the physical type is `Utf8`/`Int64`
    (UUID, varchar, bpchar, money);
  - extend `pg_type_name` for time, interval, uuid, arrays, varchar/bpchar,
    money, numeric(p,s), timestamp(p).
- Ensure parameter type inference uses `TypePlanner`/field metadata.

#### COPY FROM

Extend `copy_in.rs::decode_value`:

- `Time32(Second|Millisecond)` and `Time64(Microsecond|Nanosecond)`;
- `Interval(MonthDayNano)` (use DataFusion's interval parser where possible);
- `List` of in-scope scalar types, with PostgreSQL array literal parsing
  (`{...}`, quotes, escapes, `NULL`, empty arrays);
- `Decimal256` via a decimal text parser;
- UUID, money, varchar/char validation and normalization;
- reject unsupported element types with a clear error.

`CopyColumn` should carry the target `Field`/metadata rather than only
`DataType` and `json_kind`.

#### COPY TO

Extend `copy_out.rs::field_text`:

- `Time32/Time64`, `Interval`, `List`, `Decimal256`;
- UUID text, money text, varchar/bpchar text;
- make sure `COPY ... TO STDOUT` uses the same semantic formatting as the wire
  text protocol.

### 5.6 Catalog

- `pg_attribute` typmod/ndims as above.
- `format_type` as above.
- `pg_compat.rs::REGTYPE_OIDS` add:
  - `bpchar`/`character` 1042;
  - `inet` 869;
  - `money` 790;
  - array names for supported types (`uuid[]`, `int4[]`, `text[]`,
    `varchar[]`, `numeric[]`, `time[]`, `timestamp[]`, `interval[]`,
    `money[]`).
- `pg_type` static rows already exist for the built-in OIDs; verify the array
  rows and `typarray` links.
- `information_schema.columns` should report the same type/typmod (or a
  documented subset if it is generated separately).

### 5.7 Subscription consistency

`subscribe/source.rs::PgType` currently collapses `varchar`/`char` to `Text`
and has no interval/inet/money/array. If the target supports a new type, the
subscriber must either:

- map it with the same `pg.type`/`pg.typmod` metadata, or
- refuse it explicitly with a clear message.

At minimum, keep `UUID`, `time`, `interval`, `varchar(n)`/`char(n)` and
`Decimal256` consistent between the source and target schema. `inet` remains a
source-only unsupported type if it is deferred.

## 6. Phased delivery

### Phase 0: validation spike (1-2 days)

- Confirm Arrow `arrow.uuid` extension round-trips through
  `lakesoul_common::ser::arrow_java`, Parquet and Vortex.
- Confirm `Decimal256`, `List`, `Time32/64` and `Interval(MonthDayNano)` write
  and read through both Parquet and Vortex with the existing writer.
- Confirm DataFusion `TypePlanner` can plan the target SQL types on the current
  DataFusion version.
- Confirm `datafusion-postgres` fork changes compile and can be pinned to a new
  revision.
- Record any format that cannot round-trip; drop that type from the first cut.

Acceptance: a small test table per representation writes, commits and reads
back after a server restart.

### Phase 1: schema, DDL and catalog

- Add the type metadata module and marker constants.
- Extend `arrow_type` / `build_schema` / ALTER to understand the in-scope
  types.
- Add `PgTypePlanner` and register it.
- Extend `pg_attribute`, `format_type`, `to_regtype` and parameter overrides.
- Update refusal matrix: UUID/arrays/time/interval/money/Decimal256/timestamp(0,3)
  now accepted; unsupported precision/dimensions still `0A000`.

Acceptance: `CREATE TABLE` for every in-scope type succeeds; `\d` and
`format_type` show the right type/typmod; existing types are unchanged.

### Phase 2: native Arrow types (DML, COPY)

- Arrays: DDL, INSERT/UPDATE, COPY in/out, text wire output.
- `time(p)` for `p` 0/3/6, `interval`, `timestamp(p)` for `p` 0/3/6.
- `Decimal256` for `numeric(39..76, s)` including wire numeric encoding.
- `varchar(n)`/`char(n)` validation and catalog reporting.
- Rounding rules for time/timestamp units.

Acceptance: insert/select/COPY round-trip and differential tests against
PostgreSQL where the representation is native.

### Phase 3: UUID and money

- UUID storage as `FixedSizeBinary(16)` + `arrow.uuid`; text/binary wire;
  parameter and COPY input; array support.
- Money storage as `Decimal128(19,2)` + `pg.type=money`; text/binary wire;
  input/COPY parsing; array support.
- Prepared statement `ParameterDescription` for both.

Acceptance: `psql`, a binary client (e.g. tokio-postgres) and the
differential harness read the same values.

### Phase 4: binaries, parameters and cross-engine

- Complete array parameter decoding and binary array encoding for supported
  element types.
- Extend subscription schema/decode for the same types.
- Update docs and compatibility tables.
- Run Spark/Flink schema-read smoke tests for new tables (at least
  `FixedSizeBinary`, `List`, `Decimal256`, `Time`, `Interval`).

## 7. Test plan

### Unit

- `pg_types` mapping: SQL type -> Arrow type -> metadata -> PG OID/typmod.
- Decimal precision/scale boundaries: 38/39/76/77, zero, negative, scale 0.
- Time/timestamp unit rounding boundaries (including pre-epoch values).
- varchar/char character counting with multibyte UTF-8 and trailing spaces.
- UUID canonicalization; money parsing/formatting; array literal parser.

### DDL / catalog

- Create a table with every supported type; assert storage schema, `pg_attribute`
  OIDs, `atttypmod`, `format_type`, `to_regtype` and `pg_type`.
- Alter: add each type; widen varchar/char length, timestamp/time precision,
  Decimal128 -> Decimal256; refuse narrowing and marker changes.
- Existing refusal matrix still refuses unsupported types and dimensions.

### DML / COPY

- `INSERT`/`UPDATE`/`UPSERT` with literals and parameters.
- `COPY FROM STDIN`/`COPY TO STDOUT` text and CSV for each new type, including
  NULLs, empty arrays, array quoting and invalid input.
- Prepared statements: Parse/Describe reports the correct parameter OIDs and
  Execute decodes text and binary.
- Numeric zero/scale behavior remains correct; Decimal256 text/binary.
- UUID and money: text and binary protocol; invalid input `22P02`; length
  overflow `22001`; numeric range `22003`.

### Differential / compatibility

- Extend `tests/sql_compat` with a type matrix comparing LakeSoul against the
  reference PostgreSQL for values that are representable in both.
- Assert intentional deviations (e.g. `char(n)` padding) explicitly rather than
  claiming equality.
- Spark/Flink smoke: create a table with `List`, `Decimal256`, `Time32/64`,
  `Interval`, `FixedSizeBinary(16)` and read its schema/metadata.

## 8. Risks and open questions

1. **Fork coordination.** The cleanest wire/catalog implementation needs a
   `datafusion-postgres` fork revision. Decide whether to do the fork PR first
   or ship a local text-only shim, then replace it.
2. **Binary format correctness.** UUID and money binary encodings must be
   verified with real clients (tokio-postgres/asyncpg/psycopg), not only with
   our own tests.
3. **Decimal256 storage.** Confirm Parquet and Vortex round-trip and Spark/Flink
   read support. If Vortex cannot represent it, restrict Decimal256 to Parquet
   or document the limitation.
4. **Array lower bounds.** PostgreSQL arrays carry lower bounds; Arrow `List`
   does not. We will support standard 1-based arrays and reject/ignore custom
   lower bounds.
5. **`char(n)` semantics.** We can enforce length and report `character(n)`,
   but exact blank padding and trailing-space comparison need a separate
   decision. Phase 1 should document the deviation.
6. **DataFusion parser limits.** `TIME(p)` for `p != 6`, `TIMESTAMP(p)` for
   `p` 1/2/4/5, interval qualifiers and mixed interval syntax may remain
   unsupported at the expression level even if table columns work. Do not
   promise more than the planner supports.
7. **Metadata markers and older readers.** New markers are additive, but older
   LakeSoul/Spark/Flink readers must ignore unknown field metadata. Verify the
   Arrow Java path; do not rely on other engines understanding
   `arrow.uuid`.
8. **`inet` scope.** No Arrow-native inet type. If text-backed support is
   required, add it as a separate follow-up with explicit text/binary
   limitations.

## 9. Acceptance criteria

- `CREATE TABLE` accepts every in-scope type and `\d`/`format_type` reports the
  declared type and typmod.
- Existing table schemas and queries are unchanged.
- `INSERT`, `UPDATE`, `UPSERT`, `COPY FROM`, `COPY TO` and prepared parameters
  support the in-scope types with PostgreSQL `22P02`/`22001`/`22003`/`22007`
  errors for invalid input.
- `numeric` up to 76 digits and `timestamp(0/3/6)` work; unsupported precision
  and dimensions are refused with `0A000`.
- Text protocol is correct for every in-scope type; binary protocol works for
  native Arrow representations and is explicitly documented where deferred.
- Spark/Flink can read tables containing the new Arrow physical types without
  losing data.
- The type matrix and known deviations are documented in
  `website/docs/reference/data-types.md`,
  `website/docs/engine/postgres/ddl.md`,
  `website/docs/engine/postgres/copy.md` and the `postgres-lakesoul` README.

## 10. Key code references

- `rust/postgres-lakesoul/src/ddl.rs` - SQL type -> Arrow mapping.
- `rust/postgres-lakesoul/src/alter.rs` - add/alter column paths.
- `rust/lakesoul-common/src/schema_evolution.rs` - widening rules.
- `rust/lakesoul-datafusion/src/pg_assignment.rs` - assignment/type matrix.
- `rust/lakesoul-datafusion/src/lib.rs` - session creation/type planner hook.
- `rust/postgres-lakesoul/src/insert.rs` - INSERT/UPSERT analysis.
- `rust/postgres-lakesoul/src/copy_in.rs`, `copy_out.rs` - COPY codecs.
- `rust/postgres-lakesoul/src/pg_compat.rs` - `to_regtype` and catalog shims.
- `rust/postgres-lakesoul/src/pg_numeric.rs` - Decimal wire encoding.
- `rust/postgres-lakesoul/src/subscribe/source.rs`,
  `subscribe/schema.rs`, `subscribe/decode.rs` - subscription types.
- `datafusion-postgres/arrow-pg/src/datatypes.rs`,
  `encoder.rs`, `list_encoder.rs`, `encoder` parameter decoding -
  wire mapping and encoding.
- `datafusion-postgres/datafusion-pg-catalog/src/pg_catalog/pg_attribute.rs`,
  `format_type.rs` - catalog introspection.
