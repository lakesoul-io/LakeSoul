# env := "LAKESOUL_PG_URL=jdbc:postgresql://localhost:5532/py?stringtype=unspecified LAKESOUL_PG_USERNAME=lakesoul_test LAKESOUL_PG_PASSWORD=lakesoul_test LAKESOUL_SOURCE_DIR=/home/jiax/LakeSoul LD_LIBRARY_PATH=$JAVA_HOME/lib/server"
env := "LAKESOUL_PG_URL=jdbc:postgresql://localhost:5432/lakesoul_test?stringtype=unspecified LAKESOUL_PG_USERNAME=lakesoul_test LAKESOUL_PG_PASSWORD=lakesoul_test LAKESOUL_SOURCE_DIR=$HOME/Projects/LakeSoul LD_LIBRARY_PATH=$HADOOP_HOME/lib/native"

clean-data:
    rm -rf /tmp/lakesoul/tpch_data

gen-data:
    cd rust &&  cargo run -p lakesoul-console -- tpch-gen -p "file:///tmp/lakesoul/tpch_data" --scale-factor 0.1 -n 8 

# pytest:
#     cd .. && {{env}} uv run pytest -s 
pytest:
    {{env}} uv run pytest -s python/tests

sync:
    uv sync --reinstall-package lakesoul -vv

dist:
    uvx -p 3.8 cibuildwheel@2.19 --platform linux --output-dir dist