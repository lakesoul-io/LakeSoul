# Embodied M1-5 benchmark

Compares full-scan baselines with `lakesoul.embodied.EmbodiedDataset` window
sampling on synthetic episodes (one partition per episode):

- `baseline_full` / `embodied_full`: sequential scan and window assembly
- `baseline_subset` / `embodied_subset`: episode subset; the dataset prunes
  partitions, the baseline still reads every file
- `baseline_shuffle` / `embodied_shuffle`: shuffled epoch order
- `torch_loader`: `lakesoul.embodied.torch.Dataset` + `DataLoader`

`bytes read` is measured from the local `file://` files actually in scope for
each scenario, so the subset comparison shows the partition-pruning win.

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test

python benchmark/embodied/run_benchmark.py \
    --episodes 8 --ticks 512 --subset 2 --stride 4 \
    --torch-batches 50 --output /tmp/m1_5.json
```

Useful flags: `--image-bytes`, `--repeat` (best-of-N), `--format`,
`--num-workers`, `--keep` (keep the generated table for inspection).


## Video layout comparison (`run_video_layout_benchmark.py`)

Generates a local LeRobot v3 source with gradient frames (so the MP4 compresses
like real footage) and imports it three ways: per-frame JPEG bytes (`frames`),
Annex-B GOPs (`gop`) and the Daft frames importer (native runner). It reports
import throughput, on-disk size versus the source MP4 and raw RGB, window
sampling throughput and GOP decode latency.

```sh
python benchmark/embodied/run_video_layout_benchmark.py \
    --episodes 8 --ticks 120 --width 128 --height 128 --keyint 16 \
    --output /tmp/m2_5a.json
```

Example run: source MP4 0.16 MB; `frames` 3.55 MB (21.6x MP4), `gop` 0.42 MB
(2.54x MP4, 8.5x smaller than frames); frames sampling 8.1k samples/s vs GOP
216 samples/s with a warm decode cache (P50 0.057 ms, cold GOP decode 14 ms).
Requires the `embodied` extra (PyAV/Pillow).
