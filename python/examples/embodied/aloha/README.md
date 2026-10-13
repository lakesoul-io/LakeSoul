# ALOHA Frames pipeline with LakeSoul and Daft

This example imports `lerobot/aloha_static_thread_velcro` and processes its
Frames table into quality reports, selected intervals, dataset splits, a
Prepared table and training sample indices.

These are application examples of LakeSoul APIs, not new library APIs.
The import uses `lakesoul.embodied.daft.import_lerobot()`; QC and preparation
use `LakeSoulCatalog`, `scan().to_daft()` and `write_daft()`.

| File | Purpose |
| --- | --- |
| `import_raw.py` | Import selected episodes and two cameras; verify row counts and a few actual rows. |
| `prepare_dataset.py` | Full QC, interval selection, episode split, Prepared writes, sample indexing and sample readback. |
| `common.py` | Shared field names, argument helpers and catalog connection. |

## Environment

Use a LakeSoul installation containing the embodied Daft importer and nested
list/string normalization in its Daft sink. The PostgreSQL metadata schema
must match that installation, including any required repository migrations.

The import requires a Daft version exposing `daft.datasets.lerobot.read`
(the local import was run with Daft 0.7.25; 0.7.15 lacks this API).
The preceding QC pipeline with its Python window builder was exercised with
Daft 0.7.15 and Ray 2.55.1. This example now uses embodied `read_samples` for
window construction; that replacement still needs a run in the target environment.
Install NumPy, PyArrow and Pillow; import also needs PyAV.
Ray workers need the same LakeSoul and compatible dependencies.

Configure connections before running; replace the example endpoints and credentials:

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME='lakesoul_test'
export LAKESOUL_PG_PASSWORD='<password>'
export AWS_ENDPOINT='http://127.0.0.1:9000'
export AWS_ACCESS_KEY_ID='<access-key>'
export AWS_SECRET_ACCESS_KEY='<secret-key>'
```

For Ray, use addresses reachable from every participating Pod/node. Localhost
addresses here are only examples for a local run.

## 1. Import Raw

Run from the LakeSoul repository root with the intended Python environment
activated. Download the LeRobot v3 dataset first if it is not already present:

```bash
hf download lerobot/aloha_static_thread_velcro --repo-type dataset \
  --local-dir /mnt/robot-data/aloha_static_thread_velcro
```

Choose a new table name, storage path and import report directory:

```bash
python python/examples/embodied/aloha/import_raw.py \
  --source /mnt/robot-data/aloha_static_thread_velcro \
  --namespace default \
  --episodes 0,1,2,3,4,5,6,7,8,9 \
  --cameras cam_high,cam_left_wrist \
  --raw-table aloha_velcro_raw_v1 \
  --raw-path s3://YOUR_BUCKET/embodied/aloha_velcro_raw_v1 \
  --runner native \
  --output /tmp/aloha-import-v1
```

The importer expects 50 FPS, 14-dimensional state/action vectors and
640 × 480 images. It stores JPEG bytes in the Raw table.
Keep `/tmp/aloha-import-v1/manifest.json`: its source episode lengths are the
independent reference used by QC. Import verifies episode row counts and
five sampled rows, not full-table quality.

## 2. QC, Prepared and sample indices

`prepare_dataset.py` is the main processing script. Its `run()` function reads
an existing Raw table and performs the following stages; it does not import
the source videos again. `load_configuration()` loads expected episode lengths
from the import manifest, vector dimensions and camera sizes from `info.json`,
and task references from `tasks.parquet`.

```text
Raw + source metadata
  → frame/episode QC
  → quality intervals + optional behavior annotations → selected intervals
  → episode-level Train/Val/Test split
  → Prepared records
  → observation/action windows → Samples index → sample readback
```

### Processing stages

1. **Check data quality — `inspect_frame()`, `check_row()`, `check_episode()`.**
   Read Raw through Daft. Check state/action dimensions and finite values,
   image decoding and sizes, timestamps and task references. Aggregate by
   episode to check row counts, missing/duplicate frames and timestamp order.
   Save problems to `issues.json` and episode summaries to `quality.json`.
   These results identify unusable data before sampling.

2. **Select usable intervals — `build_quality_intervals()`, `apply_behavior_annotations()`.**
   Turn localized QC problems into invalid frame ranges; quarantine an entire
   episode when reliable local selection is not possible. Split ranges at
   optional manual annotation boundaries, then apply excluded behavior labels.
   Invalid ranges are never selected; valid, unannotated ranges remain
   `unknown` and are kept by default. `intervals.json` records quality,
   behavior, selection and reasons, including excluded ranges. Continuous
   selected runs receive a `valid_interval_id` to prevent windows crossing gaps.

3. **Assign Train/Val/Test — `split_episodes()` inside `prepare_dataset()`.**
   Shuffle eligible episode IDs with a fixed seed and assign them using the
   configured ratios, or reuse `--split-source`. All intervals from one episode
   stay in one split, preventing overlapping trajectory windows from leaking
   between training and evaluation. Save assignments to `splits.json`.

4. **Write Prepared — `select_prepared_rows()`, `prepare_dataset()`.**
   Join Raw with selected interval bounds and episode splits using Daft.
   Keep only selected frames, preserve their original frame numbers,
   timestamps and modality values, and add `split` and `valid_interval_id`.
   Write these records, including JPEG bytes, to the LakeSoul Prepared table.
   No normalization is performed here.

5. **Construct windows — `generate_training_samples()`, `build_sample_index()`.**
   Call `lakesoul.embodied.daft.read_samples()` within each selected continuous
   interval. For anchor `t`, the default configuration takes observations
   from `t-9` through `t` and actions from `t` through `t+9`. State and both
   cameras use the same observation frames: this is the multimodal sample
   assembly. Skip incomplete windows and reject missing fields or inconsistent
   frame/episode/split metadata. Save window bounds and source references to
   the Samples table; image payloads remain in Prepared.

6. **Read sample previews — `read_training_sample()`.**
   Read the windows referenced by sample indices back from Prepared and
   organize ordered state vectors, two camera JPEG sequences and action vectors
   into a Python dictionary. `--preview-samples` controls how many samples are
   checked. `qc_manifest.json` records output references, QC counts, split
   counts, sampling rules and preview lengths. This is a sample readback check,
   not Tensor batch creation or model training.

### Run the pipeline

Use the actual Raw table and its import manifest. Replace `YOUR_BUCKET`:

```bash
python python/examples/embodied/aloha/prepare_dataset.py \
  --namespace default \
  --raw-table aloha_velcro_raw_v1 \
  --raw-metadata /tmp/aloha-import-v1/manifest.json \
  --info-json /mnt/robot-data/aloha_static_thread_velcro/meta/info.json \
  --tasks /mnt/robot-data/aloha_static_thread_velcro/meta/tasks.parquet \
  --prepared-table aloha_velcro_prepared_v1 \
  --prepared-path s3://YOUR_BUCKET/embodied/aloha_velcro_prepared_v1 \
  --samples-table aloha_velcro_samples_v1 \
  --samples-path s3://YOUR_BUCKET/embodied/aloha_velcro_samples_v1 \
  --validation-ratio 0.1 --test-ratio 0.1 --seed 42 \
  --observation-steps 10 --action-steps 10 --action-offset 0 --stride 5 \
  --preview-samples 1 --runner native \
  --output /tmp/aloha-qc-v1
```

Only two output tables are created:

- Prepared: selected Raw records, with `split` and `valid_interval_id`.
- Samples: window indices referencing Prepared; no duplicated image payloads.

Intermediate results are JSON files in `--output`, not LakeSoul tables:

- `issues.json`: field-level problems and affected frame ranges.
- `quality.json`: per-episode counts and PASS/HAS_ERRORS summaries.
- `intervals.json`: quality, behavior, selection and reasons for each interval.
- `splits.json`: episode-to-train/val/test assignments.

These files are written before the later stages complete, so QC results can
still be inspected if Prepared or sample generation fails. Use a separate
report directory per run; files in an existing directory are overwritten.

The report is `/tmp/aloha-qc-v1/qc_manifest.json`. Output tables are created,
not overwritten. A failed run can leave some tables behind; use new output
table names and paths when rerunning. Raw does not need to be reimported.

Without annotations, valid intervals are kept with behavior `unknown`.
Optional manual annotation JSON:

```json
[
  {
    "episode_id": "ep000003",
    "start_frame": 0,
    "end_frame_exclusive": 40,
    "behavior": "preparation",
    "reason": "Manually confirmed initial preparation"
  }
]
```

Pass `--behavior-annotations annotations.json --exclude-behaviors preparation,idle`.
These labels are supplied by the user, not inferred by the script.
Quality-invalid intervals are always excluded. To preserve an earlier
episode split after changing annotations, pass `--split-source /path/to/splits.json`.

## 3. Ray execution

Use `--runner ray --ray-address auto` inside an existing Ray job.
Bundle the following at the root of the job working directory:

```text
prepare_dataset.py
common.py
manifest.json
info.json
tasks.parquet
```

Use relative metadata arguments:
`--raw-metadata manifest.json --info-json info.json --tasks tasks.parquet`.
The workers read image bytes from LakeSoul, so QC does not need source MP4s.
For distributed import, the source dataset must be accessible to the workers.

Daft scans, UDF checks, aggregation, joins, window construction and writes use
the Ray runner. Window construction calls `lakesoul.embodied.daft.read_samples`
with `episode_column="valid_interval_id"`, so windows cannot cross selected
interval boundaries. Interval handling, split assignment and conversion of
window metadata to persistent sample indices run on the driver. Ensure the
job and workers receive the necessary connection environment. The report
directory belongs to the job's driver node.

Use a persistent report directory or copy the JSON reports out before that
node is removed. When reusing splits in a new Ray job, include `splits.json`
in its working directory and pass `--split-source splits.json`.

## 4. Read a sample

From this example directory, with the same connection environment:

```python
from daft import col
from common import catalog_from_environment
from prepare_dataset import read_training_sample

catalog = catalog_from_environment("default")
indices = catalog.table("aloha_velcro_samples_v1").scan().to_daft()
rows = indices.where(col("split") == "train").limit(1).to_arrow().to_pylist()
if rows:
    sample = read_training_sample(catalog, rows[0])
    print({key: len(value) for key, value in sample["observation"].items()})
    print("action", len(sample["action"]))
```

Observation contains state vectors and JPEG byte lists; action contains
target vectors. Windows stay within one episode and selected continuous
interval, with no padding. `stride` moves the anchor, not the spacing
between frames within a window.

The window configuration passed to `read_samples` is `(-9, 1)` for each
observation field and `(0, 10)` for action when both lengths are 10 and the
action offset is zero. End offsets are exclusive. Metadata windows cover the
entire observation/action span; the index builder rejects gaps, duplicate
frame numbers or inconsistent episode/split values. Null modality values
are filtered on workers before collecting the lightweight metadata.

Anchors follow the API's rule: start at row zero in each interval, step by
`stride`, then skip windows crossing the boundaries. For `[0,700)`, lengths
10/10, offset 0 and stride 5, anchors are `10,15,...,690` (137 samples).
The older custom builder used `9,14,...,689`; counts can also differ for other
interval lengths. The API shuffles candidate order; saved indices are sorted
by episode, interval and anchor for stable previews. The report records the
sampler and anchor policy.

`read_samples` aggregates one interval, including its selected image fields,
on one worker and constructs overlapping windows there. Image payloads are
not collected to the driver or duplicated in the saved index table, but
worker memory usage grows with interval size and overlapping windows. This
is suitable for the small ALOHA demo; it is not a streaming DataLoader.

This example does not calculate normalization statistics, build Tensor
batches, train a model, pin a database snapshot or verify source-video
timestamp correspondence. Keep the input/output data unchanged while
processing and reading indices.
