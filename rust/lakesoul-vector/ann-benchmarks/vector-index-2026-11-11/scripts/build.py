import json, os, subprocess, sys, time
sys.path.insert(0,"/tmp/bench4")
from common import rss_mb, DATASETS
from lakesoul import LakeSoulCatalog
from lakesoul import index as idx
ds=sys.argv[1]; nlist=int(sys.argv[2]); bits=int(sys.argv[3]); d=DATASETS[ds]
cat=LakeSoulCatalog.from_env(namespace="default"); t=cat.table(d["table"])
cfg=idx.config_for_column(t._index_configs("vector"),"vec",overrides={"nlist":nlist,"total_bits":bits})
t0=time.perf_counter(); res=idx.build_table_index("vector",table_name=d["table"],namespace="default",configs=[cfg],store_config=idx.default_object_store_config(catalog=cat,table=t),rebuild=True)
elapsed=time.perf_counter()-t0
size=0
for root,_,files in os.walk(d["root"]):
 for f in files:
  if f.endswith(".seg"): size+=os.path.getsize(os.path.join(root,f))
print(json.dumps({"dataset":ds,"kind":"build","nlist":nlist,"bits":bits,"build_s":elapsed,"build_rss_mb":rss_mb(),"index_bytes":size,"status":res["status"]}),flush=True)
