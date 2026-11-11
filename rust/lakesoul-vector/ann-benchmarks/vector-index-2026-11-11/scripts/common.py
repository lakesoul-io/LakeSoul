import os
FIX="/home/zhibin/ann-benchmarks"
DATASETS={
 "sift":{"file":"sift-128-euclidean.hdf5","metric":"L2","normalize":False,"table":"vb_sift","root":"/tmp/bench4/sift","dim":128},
 "glove":{"file":"glove-200-angular.hdf5","metric":"IP","normalize":True,"table":"vb_glove","root":"/tmp/bench4/glove","dim":200},
 "gist":{"file":"gist-960-euclidean.hdf5","metric":"L2","normalize":False,"table":"vb_gist","root":"/tmp/bench4/gist","dim":960},
 "deep":{"file":"deep-image-96-angular.hdf5","metric":"IP","normalize":True,"table":"vb_deep","root":"/tmp/bench4/deep","dim":96},
}
def load(ds, train=True):
 import h5py, numpy as np
 d=DATASETS[ds]
 with h5py.File(f"{FIX}/{d['file']}","r") as f:
  tr=f["train"][:] if train else None; test=f["test"][:]; gt=f["neighbors"][:]
 if tr is not None and d["normalize"]:
  n=np.linalg.norm(tr,axis=1,keepdims=True); tr=np.divide(tr,n,out=np.zeros_like(tr),where=n>0)
 if d["normalize"]:
  n=np.linalg.norm(test,axis=1,keepdims=True); test=np.divide(test,n,out=np.zeros_like(test),where=n>0)
 return tr,test,gt
def rss_mb():
 with open("/proc/self/status") as f:
  for line in f:
   if line.startswith("VmRSS:"): return int(line.split()[1])/1024
 return 0.0
