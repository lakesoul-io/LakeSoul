env := "AWS_SECRET_ACCESS_KEY=minioadmin1 AWS_SECRET_KEY_ID=minioadmin1 AWS_ENDPOINT=http://localhost:9000 AWS_BUCKET=lakesoul-test-bucket LAKESOUL_VERSION=3.0.0-SNAPSHOT"


pytest:
     {{env}} ../../.venv/bin/python3.13 -m unittest ./test.py

run:
    uv run main.py

debug:
    python3 ./main.py --dir /Users/mag1cian/dev/internship run

test:
    {{env}} uv run main.py --fresh --repo https://github.com/mag1c1an1/LakeSoul.git --branch tmp_name run  

help:
    python3 ./main.py --help⏎      