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
