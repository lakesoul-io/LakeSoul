# SPDX-FileCopyrightText: LakeSoul Contributors

# SPDX-License-Identifier: Apache-2.0

import os
import subprocess
import shutil
from typing import Dict, List, Optional, override
from pathlib import Path
import click
import yaml


VERSION = None

LAKESOUL_GIT = "https://github.com/lakesoul-io/LakeSoul"
TMP_DIR = "/tmp/lakesoul"
CONFIG_FILE = "config.yaml"
MVN_LOCAL = "~/.m2/repository/com/dmetasoul"


class ComputeEngine:
    def __init__(self, typ: str):
        self.typ = typ

    @staticmethod
    def parse_conf(conf: str) -> "ComputeEngine":
        pass

    def run(self):
        pass


class FlinkJob:
    def __init__(self, name, entry, mode):
        self.name = name
        self.entry = entry
        self.mode = mode
        self.target = os.path.expanduser(
            f"{MVN_LOCAL}/integ.flink/{VERSION}/{name}-{VERSION}.jar"
        )

    def __repr__(self):
        return f"FlinkJob {{ {self.mode} {self.name} {self.entry} {self.target}}}"


class Flink(ComputeEngine):
    def __init__(self, conf: dict[str, str]):
        super().__init__("flink")
        self.deploy = conf["deploy"]
        self.jobs = Flink.parse_jobs(conf["jobs"])
        print(self.jobs)

    @override
    def run(self):
        for job in self.jobs:
            self.run_job(job)

    def run_job(self, job: FlinkJob):
        if self.deploy == "local":
            if job.mode == "application":
                print("WARNING: local deploy only work in session mode")
            subprocess.run(
                ["flink", "run", "-c", job.entry, job.target],
                check=True,
            )

    @override
    def parse_conf(conf: dict[str, str]) -> "Flink":
        return Flink(conf)

    def parse_jobs(conf: List[dict[str, str]]) -> List[FlinkJob]:
        res = []
        for j in conf:
            res.append(FlinkJob(j["name"], j["entry"], j["mode"]))
        return res

    def __repr__(self):
        return f"flink:{{ {self.deploy} }}"


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
    os.chdir(TMP_DIR + "/LakeSoul")

    # build lakesoul itself
    subprocess.run(
        ["mvn", "clean", "install", "-DskipTests"],
        check=True,
    )
    # build eeTests
    os.chdir("lakesoul-integ-test")
    subprocess.run(
        ["mvn", "clean", "install", "-DskipTests"],
        check=True,
    )
    os.chdir(origin)


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
@click.option("--dir", default=TMP_DIR, type=click.Path(), help="dir of lakesoul code")
@click.option("-b", "--branch", default="main", help="lakesoul branch, default is main")
def cli(ctx, conf, fresh, branch, dir):
    ctx.obj["conf"] = conf
    ctx.obj["fresh"] = fresh
    ctx.obj["branch"] = branch
    ctx.obj["dir"] = dir


def pre_run(ctx):
    # if ctx.obj["fresh"]:
    # remove dir
    # shutil.rmtree(ctx.obj["dir"])  # 如果临时目录已存在，先删除
    # os.makedirs(ctx.obj["dir"])
    # clone_repo(LAKESOUL_GIT, ctx.obj["branch"], ctx.obj["dir"])

    with open(ctx.obj["conf"]) as f:
        ctx.obj["config"] = yaml.safe_load(f)

    # build lakesoul
    # build(ctx.obj["dir"])

    global VERSION
    VERSION = ctx.obj["config"]["version"]


@click.command()
@click.pass_context
@click.argument("engine", type=click.Choice(["flink", "spark", "presto"]))
def run(ctx, engine):
    # args correct
    pre_run(ctx)
    if engine == "flink":
        flink = Flink.parse_conf(ctx.obj["config"]["flink"])
        print(flink)
        flink.run()


if __name__ == "__main__":
    cli.add_command(run)
    cli(obj={})
