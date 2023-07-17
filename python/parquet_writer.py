# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

def write_file():
    """Original commands used to write 'large_file.parquet'

    This locally-written file was moved to S3 and GCS using `fs.put`.
    """
    from dask.datasets import timeseries

    dtypes = {}
    # dtypes.update({f"str{i}": str for i in range(15)})
    dtypes.update({f"int{i}": int for i in range(15)})

    for seed in range(3):
        df = timeseries(
            start='2000-12-02',
            end='2000-12-03',
            freq='1800s',
            partition_freq='1d',
            dtypes=dtypes,
            seed=seed,
        ).reset_index(drop=True).compute()

        print(len(df))
        row_group_size = (len(df) // 5)  # Want 10 row groups

        df.to_parquet(f"small_{seed}.parquet", engine="pyarrow", row_group_size=row_group_size)

if __name__ == '__main__':
    write_file()