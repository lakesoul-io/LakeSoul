from lakesoul._lib._metadata import exec_query
from lakesoul._lib._dataset import sync_reader

from lakesoul.arrow.dataset import Dataset


def test():
    print("that's it ")


def test_dataset():
    ds = Dataset(lakesoul_table_name="orders")
    for rb in ds.to_batches():
        print("???")
        print(rb)
