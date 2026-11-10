import json, sys, time
sys.path.insert(0,"/tmp/annbench")
from common import *
from lakesoul import LakeSoulCatalog
from lakesoul import index as idx
name=sys.argv[1]; nlist=int(sys.argv[2]); bits=int(sys.argv[3]); d=DATASETS[name]
cat=LakeSoulCatalog.from_env(object_store_options=OPTIONS); t=cat.table(d["table"])
config=idx.config_for_column(t._index_configs("vector"),"vec",overrides={"nlist":nlist,"total_bits":bits})
t0=time.perf_counter(); res=idx.build_table_index("vector",table_name=d["table"],namespace="default",configs=[config],store_config=idx.default_object_store_config(catalog=cat,table=t),rebuild=True)
print(json.dumps({"dataset":name,"nlist":nlist,"bits":bits,"build_s":time.perf_counter()-t0,"status":res["status"]}),flush=True)
