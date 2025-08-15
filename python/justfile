env := "LAKESOUL_PG_URL=jdbc:postgresql://localhost:5532/py?stringtype=unspecified LAKESOUL_PG_USERNAME=lakesoul_test LAKESOUL_PG_PASSWORD=lakesoul_test LAKESOUL_SOURCE_DIR=/home/jiax/LakeSoul LD_LIBRARY_PATH=$JAVA_HOME/lib/server"

clean-data:
    rm -rf /tmp/lakesoul/tpch_data

gen-data:
    cd ../rust && {{env}} cargo run -p lakesoul-console -- tpch-gen -p "file:///tmp/lakesoul/tpch_data" --scale-factor 0.1 -n 8 

pytest:
    cd .. && {{env}} uv run pytest -s 
