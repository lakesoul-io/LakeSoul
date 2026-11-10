import sys
sys.path.insert(0,"/tmp/bench2")
from common2 import *
from lakesoul import LakeSoulCatalog
name=sys.argv[1]; d=DATASETS[name]
cat=LakeSoulCatalog.from_env(object_store_options=OPTIONS)
cat.drop_table(d["table"],if_exists=True)
print("dropped",d["table"])
