import json, sys, time
import numpy as np
sys.path.insert(0,"/tmp/bench2")
from common2 import *
from lakesoul import LakeSoulCatalog
name=sys.argv[1]; nlist=int(sys.argv[2]); bits=int(sys.argv[3]); nprobe=int(sys.argv[4]); cache=sys.argv[5]
d=DATASETS[name]; TOP=100; NQ=d["nq"] if cache=="on" else d["nq_off"]
train,test,gt=load_split(name)
# The search process only needs queries and ground truth.  Holding `train`
# would add the full base-vector array (3.8 GB for GIST/Deep) to RSS and make
# the measurement look like search memory instead of benchmark-script memory.
del train
qs=test[:NQ]; gt=gt[:NQ,:TOP]
cat=LakeSoulCatalog.from_env(object_store_options=OPTIONS); s=cat.session(vector_search={"rerank":"index"},object_store_options=OPTIONS,warehouse_prefix=f"s3://{BUCKET}/"); t=cat.table(d["table"])
def search(q,rerank):
 return t.search(q.tolist(),top_k=TOP,rerank=rerank,nprobe=nprobe,exact_overfetch=1,metric=d["metric"],session=s).column("id").to_pylist()
def rec(ids):
 return float(np.mean([len(set(ids[i]) & set(int(x) for x in gt[i]))/TOP for i in range(NQ)]))
def batch_search(exact):
 if cache == "off" and exact:
  return t.search_batch(qs, column="vec", top_k=TOP, rerank="exact", nprobe=nprobe, exact_overfetch=1, metric=d["metric"], session=s)
 return s.batch_search(d["table"],qs,column="vec",top_k=TOP,nprobe=nprobe,parallelism=1,batch_size=NQ,exact=exact,exact_overfetch=1,metric=d["metric"])
out={"dataset":name,"nlist":nlist,"bits":bits,"nprobe":nprobe,"cache":cache,"nq":NQ,"metric":d["metric"]}
for rerank in ("index","exact"):
 t0=time.perf_counter(); first=search(qs[0],rerank); out[f"{rerank}_first_ms"]=(time.perf_counter()-t0)*1000
 for q in qs[:min(5,NQ)]: search(q,rerank)
 t0=time.perf_counter(); ids=[search(q,rerank) for q in qs]; elapsed=time.perf_counter()-t0
 out[f"{rerank}_qps"]=NQ/elapsed; out[f"{rerank}_mean_ms"]=elapsed/NQ*1000; out[f"{rerank}_recall"]=rec(ids); out[f"{rerank}_rss_mb"]=rss()["VmRSS"]
for exact in (False,True):
 label="exact" if exact else "index"
 try:
  t0=time.perf_counter(); b=batch_search(exact); out[f"{label}_batch_first_ms"]=(time.perf_counter()-t0)*1000
  b=batch_search(exact)
  t0=time.perf_counter(); rounds=2
  for _ in range(rounds): b=batch_search(exact)
  elapsed=time.perf_counter()-t0
  out[f"{label}_batch_qps"]=NQ*rounds/elapsed; out[f"{label}_batch_mean_ms"]=elapsed/(NQ*rounds)*1000; out[f"{label}_batch_recall"]=rec(b.ids); out[f"{label}_batch_rss_mb"]=rss()["VmRSS"]
 except Exception as error:
  out[f"{label}_batch_error"]=str(error)[:300]
print(json.dumps(out),flush=True)
