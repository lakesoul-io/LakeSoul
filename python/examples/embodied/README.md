# Embodied data examples

Synthetic embodied episodes with `state` / `action` vectors and per-frame
`image` bytes, laid out with one partition per episode (the M1 hard
constraint) so episode selection prunes files at metadata level.

## Setup

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test

cd python
uv sync --group dev        # or `uv run maturin develop` for an in-place build
```

## Generate data

```sh
python python/examples/embodied/generate_data.py \
    --table embodied_demo --episodes 8 --ticks 512 --overwrite
```

## Train

```sh
python python/examples/embodied/train.py --table embodied_demo --epochs 2
```

The script selects episodes, builds state/action windows around anchor ticks
and feeds them through `lakesoul.embodied.torch.Dataset` (shuffle buffer +
prefetch) into a linear model.

## Compare with the full-scan baseline

```sh
python benchmark/embodied/run_benchmark.py --episodes 8 --ticks 512 \
    --output /tmp/m1_5.json
```
