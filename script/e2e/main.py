# SPDX-FileCopyrightText: LakeSoul Contributors

# SPDX-License-Identifier: Apache-2.0

import os
import subprocess
import shutil
from typing import Dict, List, Optional, override, Any
from pathlib import Path
import click
import yaml

from pathlib import Path


VERSION = None

LAKESOUL_GIT = "https://github.com/lakesoul-io/LakeSoul.git"
TMP_CODE_DIR = Path("/tmp/lakesoul/code")
CONFIG_FILE = "config.yaml"
MVN_LOCAL = Path("~/.m2/repository/com/dmetasoul")

E2E_DATA_DIR = Path("/tmp/lakesoul/e2e/data")


class SubTask:
    def run(self, **conf):
        pass


class CsvRenameSubTask(SubTask):
    global E2E_DATA_DIR

    def run(self, **conf):
        all_items = os.listdir("/tmp/lakesoul/e2e/data")
        print(all_items)
        if len(all_items) != 1:
            raise RuntimeError("data init failed")

        os.rename(E2E_DATA_DIR / all_items[0], E2E_DATA_DIR / "data.csv")


class FlinkSubTask(SubTask):

    def __init__(self, name, entry, mode):
        self.name = name
        self.entry = entry
        self.mode = mode
        self.target = os.path.expanduser(
            f"{MVN_LOCAL}/flink-e2e/{VERSION}/flink-e2e-{VERSION}.jar"
        )

    def run(self, **conf):
        args = ["flink", "run", "-c", self.entry, self.target]
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
    print(f"Cloning repository from {repo_url} (branch: {branch})...")
    origin = os.getcwd()
    os.chdir(dir)
    subprocess.run(
        ["git", "clone", "--depth=1", "--branch", branch, repo_url],
        check=True,
    )
    os.chdir(origin)
    print("Repository cloned successfully.")


def build(dir: str):
    """编译LakeSoul

    Args:
        dir (str): 代码目录
    """
    origin = os.getcwd()
    os.chdir(dir)

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
    print(conf)
    if conf["type"] == "flink":
        return FlinkSubTask(conf["name"], conf["entry"], conf["mode"])
    elif conf["type"] == "spark":
        return SparkSubTask(conf["name"], conf["entry"], conf["mode"])
    else:
        raise RuntimeError("Unsupported Engine")


def combine_subtasks(sinks: List[SubTask], souce: List[SubTask]) -> List[Task]:

    res = []
    for sk in sinks:
        for se in souce:
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
    init_rename_data = CsvRenameSubTask()
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
def cli(ctx, conf, fresh, repo, branch, dir):
    ctx.obj["conf"] = conf
    ctx.obj["fresh"] = fresh
    ctx.obj["branch"] = branch
    ctx.obj["dir"] = Path(dir)
    ctx.obj["repo"] = repo


def pre_run(ctx):

    if ctx.obj["fresh"]:
        # remove dir
        if ctx.obj["dir"].exists():
            shutil.rmtree(ctx.obj["dir"])  # 如果临时目录已存在，先删除
        os.makedirs(ctx.obj["dir"])
        clone_repo(ctx.obj["repo"], ctx.obj["branch"], ctx.obj["dir"])

    try:
        shutil.rmtree("/tmp/lakesoul/e2e/data")
    except FileNotFoundError:
        # ignore
        pass
    os.makedirs("/tmp/lakesoul/e2e/data")

    with open(ctx.obj["conf"]) as f:
        ctx.obj["config"] = yaml.safe_load(f)

    # build lakesoul
    build(ctx.obj["dir"] / "LakeSoul")

    global VERSION
    VERSION = ctx.obj["config"]["version"]


@click.command()
@click.pass_context
def run(ctx):
    # args correct
    pre_run(ctx)
    tasks = parse_conf(ctx.obj["config"])
    runner = TaskRunner(tasks)
    runner.run()


if __name__ == "__main__":
    cli.add_command(run)
    cli(obj={})