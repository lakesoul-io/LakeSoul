import json, os, sys, time
import numpy as np
sys.path.insert(0,"/tmp/annbench")
from common import *
from lakesoul import LakeSoulCatalog
name=sys.argv[1]; nlist=int(sys.argv[2]); bits=int(sys.argv[3]); nprobe=int(sys.argv[4]); do_sql=("sql" in sys.argv)
d=DATASETS[name]; NQ=d["nq"]; TOP=100
qs=load_queries(name,NQ)
def rss():
 out={}
 with open("/proc/self/status") as f:
  for line in f:
   if line.startswith(("VmRSS:","VmHWM:")):
    k,v=line.split(":"); out[k]=int(v.split()[0])/1024
 return out
cat=LakeSoulCatalog.from_env(object_store_options=OPTIONS)
s=cat.session(vector_search={"rerank":"index"},object_store_options=OPTIONS,warehouse_prefix=f"s3://{BUCKET}/")
t=cat.table(d["table"])
def search(q,rerank):
 return t.search(q.tolist(),top_k=TOP,rerank=rerank,nprobe=nprobe,exact_overfetch=1,metric=d["metric"],session=s)
def batch(exact):
 return s.batch_search(d["table"],qs,column="vec",top_k=TOP,nprobe=nprobe,parallelism=1,batch_size=NQ,exact=exact,exact_overfetch=1,metric=d["metric"])
out={"dataset":name,"nlist":nlist,"bits":bits,"nprobe":nprobe,"nq":NQ,"top_k":TOP,"metric":d["metric"]}
# single index
t0=time.perf_counter(); first=search(qs[0],"index"); out["index_single_first_ms"]=(time.perf_counter()-t0)*1000
for q in qs[:10]: search(q,"index")
t0=time.perf_counter()
for q in qs: search(q,"index")
out["index_single_qps"]=NQ/(time.perf_counter()-t0); out["index_single_rss_mb"]=rss()["VmRSS"]
# batch index
t0=time.perf_counter(); b=batch(False); out["index_batch_first_ms"]=(time.perf_counter()-t0)*1000
batch(False); t0=time.perf_counter()
for _ in range(3): batch(False)
out["index_batch_qps"]=(NQ*3)/(time.perf_counter()-t0); out["index_batch_rss_mb"]=rss()["VmRSS"]
# single exact
t0=time.perf_counter(); first=search(qs[0],"exact"); out["exact_single_first_ms"]=(time.perf_counter()-t0)*1000
for q in qs[:10]: search(q,"exact")
t0=time.perf_counter()
for q in qs: search(q,"exact")
out["exact_single_qps"]=NQ/(time.perf_counter()-t0); out["exact_single_rss_mb"]=rss()["VmRSS"]
# batch exact
t0=time.perf_counter(); b=batch(True); out["exact_batch_first_ms"]=(time.perf_counter()-t0)*1000
batch(True); t0=time.perf_counter()
for _ in range(3): batch(True)
out["exact_batch_qps"]=(NQ*3)/(time.perf_counter()-t0); out["exact_batch_rss_mb"]=rss()["VmRSS"]
# optional SQL loop (DataFusion frontend) for one representative point
if do_sql:
    s.set_vector_search(rerank="index",nprobe=nprobe,exact_overfetch=1)
    # use search() for warmup, then raw SQL loop
    for q in qs[:5]: search(q,"index")
    t0=time.perf_counter()
    for q in qs:
        s.sql(t.to_sql(q.tolist(),top_k=TOP,metric=d["metric"],columns=["id"]))
    out["sql_index_single_qps"]=NQ/(time.perf_counter()-t0); out["sql_index_rss_mb"]=rss()["VmRSS"]
    s.set_vector_search(rerank="exact",nprobe=nprobe,exact_overfetch=1)
    for q in qs[:5]: search(q,"exact")
    t0=time.perf_counter()
    for q in qs:
        s.sql(t.to_sql(q.tolist(),top_k=TOP,metric=d["metric"],columns=["id"]))
    out["sql_exact_single_qps"]=NQ/(time.perf_counter()-t0); out["sql_exact_rss_mb"]=rss()["VmRSS"]
print(json.dumps(out),flush=True)
