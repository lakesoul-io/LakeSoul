from lakesoul.arrow import lakesoul_dataset
import time

ds = lakesoul_dataset("test_lakesoul", batch_size=10240)

total_rows = 0
total_cols = 0
for batch in ds.to_batches():
    print(batch)
    total_rows += batch.num_rows
    total_cols += batch.num_columns
print(total_rows, total_cols)
print(ds.to_table())
