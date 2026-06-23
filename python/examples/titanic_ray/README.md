# Titanic ML Pipeline — Ray Data + Ray Train

This example demonstrates an end-to-end ML pipeline using LakeSoul + Ray, without Spark.

## Prerequisites

1. Set up environment variables:
   ```bash
   source lakesoul_env.sh
   ```

2. Create the `titanic_raw` LakeSoul table:
   ```bash
   python examples/import_titanic.py
   ```

3. Install dependencies:
   ```bash
   pip install lakesoul[ray,torch]
   ```

## Run

```bash
python examples/titanic_ray/train_ray.py
```

## Pipeline

```
catalog.scan("titanic_raw").to_ray()
  → map_batches(extract_title)
  → map_batches(categorize_family)
  → map_batches(impute_missing)
  → map_batches(one_hot_and_assemble)
  → select_columns(["features", "label"])
  → train_test_split(90/10)
  → TorchTrainer(DNN)
```

## Comparison with Spark Pipeline

| Dimension | Spark (`examples/titanic/`) | Ray (this example) |
|-----------|----------------------------|---------------------|
| Feature engineering | PySpark UDF (JVM) | Ray Data `map_batches` (Python) |
| Training | Native PyTorch on materialized data | Ray Train `TorchTrainer` |
| Debugging | Spark UI | Local breakpoints |
| Dependencies | Spark cluster | `pip install lakesoul[ray,torch]` |
