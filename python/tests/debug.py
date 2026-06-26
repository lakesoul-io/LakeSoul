import pyarrow.compute as pc

from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()

# ds = catalog.table("test_lfs").scan().to_arrow_dataset()
# print(ds.to_table())
# ds = catalog.table("test_lfs").scan(partitions={"c2": "2"}).to_arrow_dataset()
# print(ds.to_table())
# ds = catalog.table("test_lfs").scan(filter=pc.field("c2") == 3).to_arrow_dataset()
# print(ds.to_table())


ds = catalog.table("part").scan().to_arrow_dataset()

for b in ds.to_batches():
    print(b)
