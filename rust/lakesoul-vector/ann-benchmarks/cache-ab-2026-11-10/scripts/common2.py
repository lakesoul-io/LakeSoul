import os
BUCKET="lakesoul-bench2"
OPTIONS={"fs.s3a.bucket":BUCKET,"fs.s3a.access.key":"minioadmin","fs.s3a.secret.key":"minioadmin","fs.s3a.endpoint":"http://127.0.0.1:9000","fs.s3a.path.style.access":"true"}
FIX="/home/zhibin/ann-benchmarks"
DATASETS={
 "sift":{"file":"sift-128-euclidean.hdf5","metric":"L2","normalize":False,"table":"bench2_sift","nlist":[256,1024],"nq":100,"nq_off":20},
 "glove":{"file":"glove-200-angular.hdf5","metric":"IP","normalize":True,"table":"bench2_glove","nlist":[256,1024],"nq":100,"nq_off":20},
 "gist":{"file":"gist-960-euclidean.hdf5","metric":"L2","normalize":False,"table":"bench2_gist","nlist":[256,1024],"nq":100,"nq_off":20},
 "deep":{"file":"deep-image-96-angular.hdf5","metric":"IP","normalize":True,"table":"bench2_deep","nlist":[1024,4096],"nq":50,"nq_off":20},
}
def load_split(name):
 import h5py, numpy as np
 d=DATASETS[name]
 with h5py.File(f"{FIX}/{d['file']}","r") as f:
  train=f["train"][:]; test=f["test"][:]; gt=f["neighbors"][:]
 if d["normalize"]:
  n=np.linalg.norm(train,axis=1,keepdims=True); train=np.divide(train,n,out=np.zeros_like(train),where=n>0)
  n=np.linalg.norm(test,axis=1,keepdims=True); test=np.divide(test,n,out=np.zeros_like(test),where=n>0)
 return train,test,gt
def rss():
 out={}
 with open("/proc/self/status") as f:
  for line in f:
   if line.startswith(("VmRSS:","VmHWM:")):
    k,v=line.split(":"); out[k]=int(v.split()[0])/1024
 return out
