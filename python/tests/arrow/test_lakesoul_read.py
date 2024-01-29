from lakesoul.arrow import lakesoul_dataset

ds = lakesoul_dataset("test_lakesoul_table", batch_size=10240)

total_rows = 0
total_cols = 0
for batch in ds.to_batches():
    total_rows += batch.num_rows
    total_cols += batch.num_columns
print(total_rows, total_cols)