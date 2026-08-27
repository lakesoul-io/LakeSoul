import pyarrow.compute as pc

from lakesoul import LakeSoulCatalog

# build lakesoul catalog
catalog = LakeSoulCatalog.from_env()
# build lakesoul scan
scan = catalog.table("test_lfs").scan()

# arrow
ds = scan.to_arrow_dataset()
# huggingface
# scan.to_huggingface()
# ray
# scan.to_ray()
# daft
# scan.to_daft()

# convert to pands table
df = ds.to_table().to_pandas()
print(df)

# ds = catalog.table("test_lfs").scan().to_arrow_dataset()
# print(ds.to_table())
# ds = catalog.table("test_lfs").scan(partitions={"c2": "2"}).to_arrow_dataset()
# print(ds.to_table())
# ds = catalog.table("test_lfs").scan(filter=pc.field("c2") == 3).to_arrow_dataset()
# print(ds.to_table())


ds = catalog.table("part").scan().to_arrow_dataset()

for b in ds.to_batches():
    print(b)
