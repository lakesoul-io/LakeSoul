# SPDX-FileCopyrightText: LakeSoul Contributors

# SPDX-License-Identifier: Apache-2.0

import os
import subprocess
import shutil
from typing import Dict, List, Optional, override, Any
from pathlib import Path
import boto3
import click
import yaml
import logging

from pathlib import Path


VERSION = os.getenv("LAKESOUL_VERSION",None)
LAKESOUL_GIT = "https://github.com/lakesoul-io/LakeSoul.git"
TMP_CODE_DIR = Path("/tmp/lakesoul/e2e/code")
CONFIG_FILE = "config.yaml"
MVN_LOCAL = Path("~/.m2/repository/com/dmetasoul")

FLINK_VERSION = "1.20"
SPARK_VERSION = "3.3"

# lakesoul-flink
LAKESOUL_FLINK_PATH = Path(os.path.expanduser( f"{MVN_LOCAL}/lakesoul-flink/{FLINK_VERSION}-{VERSION}/lakesoul-flink-{FLINK_VERSION}-{VERSION}.jar"))
# lakesoul-spark
LAKESOUL_SPARK_PATH = Path(os.path.expanduser( f"{MVN_LOCAL}/lakesoul-spark/{SPARK_VERSION}-{VERSION}/lakesoul-spark-{SPARK_VERSION}-{VERSION}.jar"))

# s3
END_POINT =  os.getenv("END_POINT",None)
ACCESS_KEY =os.getenv("ACCESS_KEY",None) 
SECRET_KEY =os.getenv("SECRET_KEY",None) 
BUCKET = os.getenv("BUCKET",None)
S3_CLIENT= boto3.client(  
        's3',  
        aws_access_key_id=ACCESS_KEY,  
        aws_secret_access_key=SECRET_KEY,  
        # 下面给出一个endpoint_url的例子  
        endpoint_url=END_POINT
        )  

E2E_DATA_DIR = "lakesoul/e2e/data"
E2E_CLASSPATH ="m2"
E2E_CLASSPATH_WITH_ENDPOINT = f"{END_POINT}/{BUCKET}/{E2E_CLASSPATH}"



class SubTask:
    def run(self, **conf):
        pass

class CheckParquetSubTask(SubTask):
    global E2E_DATA_DIR

    def run(self, **conf):
        all_items = os.listdir("/tmp/lakesoul/e2e/data")
        print(all_items)
        if len(all_items) != 1:
            raise RuntimeError("data init failed")
        # os.rename(E2E_DATA_DIR / all_items[0], E2E_DATA_DIR / "data.csv")


class FlinkSubTask(SubTask):

    def __init__(self, name, entry, mode):
        self.name = name
        self.entry = entry
        self.mode = mode
        self.target = os.path.expanduser(
            f"{MVN_LOCAL}/flink-e2e/{VERSION}/flink-e2e-{VERSION}.jar"
        )

        p = os.path.expanduser(
            f"{MVN_LOCAL}/lakesoul-flink/{FLINK_VERSION}-{VERSION}/lakesoul-flink-{FLINK_VERSION}-{VERSION}.jar"
        )
        self.lib = f"file://{p}"

    def run(self, **conf):
        args = ["flink", "run","--classpath",self.lib, "-c", self.entry, self.target]
        subprocess.run(args, check=True)

    def __repr__(self):
        return f"FlinkSubTask {{ {self.mode} {self.name} {self.entry} {self.target}}}"


class SparkSubTask(SubTask):

    def __init__(self, name, entry, mode):
        self.name = name
        self.entry = entry
        self.mode = mode
        self.target = os.path.expanduser(
            f"{MVN_LOCAL}/spark-e2e/{VERSION}/spark-e2e-{VERSION}.jar"
        )

    def run(self, **conf):
        args = [
            "spark-submit",
            "--class",
            self.entry,
            "--master",
            "local[*]",
            self.target,
        ]
        subprocess.run(args, check=True)

    def __repr__(self):
        return f"SparkSubTask {{ {self.mode} {self.name} {self.entry} {self.target}}}"


class Task:
    def __init__(self, sink: Optional[SubTask], source: Optional[SubTask]) -> None:
        self.sink = sink
        self.source = source

    def run(self):
        if self.sink:
            self.sink.run()
        if self.source:
            self.source.run()


class TaskRunner:
    def __init__(self, tasks: List[Task]) -> None:
        self.tasks = tasks

    def run(self):
        for t in self.tasks:
            t.run()


def clone_repo(repo_url, branch, dir) -> None:
    """从 Git 仓库拉取代码"""
    logging.info(f"Cloning repository from {repo_url} (branch: {branch})...")
    origin = os.getcwd()
    os.chdir(dir)
    subprocess.run(
        ["git", "clone", "--depth=1", "--branch", branch, repo_url],
        check=True,
    )
    os.chdir(origin)
    logging.info("Repository cloned successfully.")


def build_install(base_dir: str):
    """build LakeSoul

    Args:
        dir (str): base dir of LakeSoul
    """
    origin = os.getcwd()
    os.chdir(base_dir)

    # build lakesoul itself
    subprocess.run(
        ["mvn", "clean", "install", "-DskipTests"],
        check=True,
    )
    # build e2eTests
    os.chdir("lakesoul-integ-test")
    subprocess.run(
        ["mvn", "clean", "install", "-DskipTests"],
        check=True,
    )
    os.chdir(origin)


def parse_subtask(conf: Dict[str, Any]) -> SubTask:
    logging.debug(conf)
    if conf["type"] == "flink":
        return FlinkSubTask(conf["name"], conf["entry"], conf["mode"])
    elif conf["type"] == "spark":
        return SparkSubTask(conf["name"], conf["entry"], conf["mode"])
    else:
        raise RuntimeError("Unsupported Engine")


def combine_subtasks(sinks: List[SubTask], source: List[SubTask]) -> List[Task]:

    res = []
    for sk in sinks:
        for se in source:
            res.append(Task(sk, se))

    return res


def parse_subtasks(conf: List[Dict[str, Any]]) -> List[SubTask]:
    print(conf)
    print(type(conf))
    res = []
    for t in conf:
        res.append(parse_subtask(t))
    return res


def parse_conf(conf: Dict[str, Any]) -> List[Task]:
    init_data_gen = parse_subtask(conf["init"])
    init_rename_data = CheckParquetSubTask()
    init_task = Task(init_data_gen, init_rename_data)

    sinks = parse_subtasks(conf["sinks"])
    sources = parse_subtasks(conf["sources"])

    tasks = [init_task] + combine_subtasks(sinks, sources)
    return tasks


# cli

@click.group()
@click.pass_context
@click.option(
    "-f",
    "--conf",
    default="config.yaml",
    type=click.Path(exists=True),
    help="config path",
)
@click.option("--fresh", is_flag=True, help="fresh clone lakesoul repo")
@click.option("--repo", default=LAKESOUL_GIT, help="LakeSoul repo")
@click.option(
    "--dir", default=TMP_CODE_DIR, type=click.Path(), help="dir of lakesoul code"
)
@click.option("-b", "--branch", default="main", help="lakesoul branch, default is main")
@click.option("--log", default="INFO",help="log level")
def cli(ctx, conf, fresh, repo, branch, dir,log):
    ctx.obj["conf"] = conf
    ctx.obj["fresh"] = fresh
    ctx.obj["branch"] = branch
    ctx.obj["dir"] = Path(dir)
    ctx.obj["repo"] = repo
    ctx.obj['log'] =log


def s3_upload_jars():
    """ upload installed lakesoul jars to s3
    """
    jars:List[Path] = [
        LAKESOUL_FLINK_PATH,
        LAKESOUL_SPARK_PATH
    ]

    for jar in jars:
        with open(jar,'rb') as data:
            S3_CLIENT.put_object(Bucket=BUCKET,Key=f"{E2E_CLASSPATH}/{jar.name}",Body=data,StorageClass='STANDARD')

def s3_delete_jars():
    """delete jars at s3
    """
    jars:List[Path] = [
        LAKESOUL_FLINK_PATH,
        LAKESOUL_SPARK_PATH
    ]
    for jar in jars:
        S3_CLIENT.delete_object(Bucket=BUCKET,Key=f"{E2E_CLASSPATH}/{jar.name}")
    
def s3_delete_dir(dir:str):
    """delete a dir in s3 like local file system

    Args:
        dir (str): _description_
    """
    resp = S3_CLIENT.list_objects(Bucket=BUCKET,Prefix=dir)
    objs = []
    try:

        for obj in resp['Contents']:
            objs.append({
                'Key':obj['Key']
            })
        S3_CLIENT.delete_objects(Bucket=BUCKET,Delete={'Objects':objs})
    except KeyError:
        logging.info(f"[INFO] s3://{BUCKET}/{dir} is empty")

def init_log(loglevel:str):
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level)


def pre_run(ctx):
    init_log(ctx['log'])
    if ctx.obj["fresh"]:
        # remove dir
        if ctx.obj["dir"].exists():
            shutil.rmtree(ctx.obj["dir"])  # 如果临时目录已存在，先删除
        os.makedirs(ctx.obj["dir"])
        clone_repo(ctx.obj["repo"], ctx.obj["branch"], ctx.obj["dir"])

    s3_delete_dir(E2E_DATA_DIR)
        
    with open(ctx.obj["conf"]) as f:
        ctx.obj["config"] = yaml.safe_load(f)

    # build lakesoul
    # build_install(ctx.obj["dir"] / "LakeSoul")


@click.command()
@click.pass_context
def run(ctx):
    # args correct
    # init log
    pre_run(ctx)
    tasks = parse_conf(ctx.obj["config"])
    runner = TaskRunner(tasks)
    runner.run()


if __name__ == "__main__":
    print(Path('s3://lakesoul-test-bucket') / "???")
    print(LAKESOUL_SPARK_PATH.name)
    # cli.add_command(run)
    # cli(obj={})