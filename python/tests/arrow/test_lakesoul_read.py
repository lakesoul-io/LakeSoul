from lakesoul.arrow import lakesoul_dataset


def test_lakesoul_dataset():
    # before this make share tpch data(0.1) is ready
    lds = lakesoul_dataset("part", batch_size=10240)

    total_rows = 0
    total_cols = 0
    for batch in lds.to_batches():
        total_rows += batch.num_rows
        assert batch.num_columns == 9
        total_cols += batch.num_columns

    assert total_rows == 20000
