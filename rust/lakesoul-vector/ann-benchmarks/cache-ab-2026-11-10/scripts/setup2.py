import json, sys, time
import numpy as np, pyarrow as pa
sys.path.insert(0,"/tmp/bench2")
from common2 import *
from lakesoul import LakeSoulCatalog
name=sys.argv[1]; d=DATASETS[name]
train,test,gt=load_split(name); rows,dim=train.shape
cat=LakeSoulCatalog.from_env(object_store_options=OPTIONS)
cat.drop_table(d["table"],if_exists=True)
schema=pa.schema([pa.field("id",pa.int64(),False),pa.field("vec",pa.list_(pa.field("item",pa.float32()),dim),False)])
t=cat.create_table(d["table"],path=f"s3://{BUCKET}/{d['table']}",schema=schema,primary_keys=["id"],hash_bucket_num=4,vector_index=[{"column":"vec","dim":dim,"nlist":1024,"total_bits":1,"metric":d["metric"]}])
t0=time.perf_counter()
t.write_arrow(pa.table({"id":pa.array(np.arange(rows,dtype=np.int64)),"vec":pa.FixedSizeListArray.from_arrays(pa.array(train.reshape(-1),type=pa.float32()),dim)}),format="vortex",auto_build_vector_index=False)
print(json.dumps({"dataset":name,"rows":rows,"dim":dim,"write_s":time.perf_counter()-t0,"rss":rss(),"gt_queries":len(gt)}),flush=True)
