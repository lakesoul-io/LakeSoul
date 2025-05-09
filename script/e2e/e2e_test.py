# SPDX-FileCopyrightText: LakeSoul Contributors

# SPDX-License-Identifier: Apache-2.0


import os
import subprocess
import shutil
import sys
import argparse
from typing import Dict, List, override
import xml.etree.ElementTree as ET
from pathlib import Path
import json

VERSION = None

GITHUB_URL = "https://github.com/lakesoul-io/LakeSoul"
BRANCH = "main"
TMP_DIR = "/tmp/lakesoul/code"
CONFIG_FILE = "./config.json"

MODULES = ["flink", "spark", "presto"]

MVN_LOCAL = "~/.m2/repository/com/dmetasoul"


class Job:
    def __init__(self, typ: str):
        self.typ = typ

    def run(self):
        pass


class FlinkJob(Job):
    def __init__(self, mode: str, name: str, entry: str):
        global VERSION
        super().__init__("flink")
        self.mode = mode
        self.name = name
        self.enty = entry
        self.target = os.path.expanduser(
            f"{MVN_LOCAL}/integ.{self.typ}/{VERSION}/{name}-{VERSION}.jar"
        )

    @override
    def run(self):
        if self.mode == "cluster":
            subprocess.run(
                ["flink", "run", "-c", self.enty, self.target],
                check=True,
            )


def get_version(code_dir: str) -> str:
    dir = Path(code_dir)
    # 解析 XML 文件
    tree = ET.parse(dir.joinpath("pom.xml"))
    root = tree.getroot()
    #
    # # 提取基本信息

    namespaces = {"m": "http://maven.apache.org/POM/4.0.0"}
    props = root.find("m:properties", namespaces=namespaces)
    version = props.findtext("m:revision", namespaces=namespaces)

    #
    print(f"Version: {version}")
    return version


def add_flink_conf():
    """now flink e2e test need jvm -ea"""
    pass


def add_lakesoul():
    pass


def add_lakesoul_for_presto():
    pass


def add_lakesoul_for_spark():
    pass


def add_lakesoul_for_flink():
    pass


def clone_repo(repo_url, branch, temp_dir) -> None:
    """从 Git 仓库拉取代码"""
    print(f"Cloning repository from {repo_url} (branch: {branch})...")
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)  # 如果临时目录已存在，先删除
    os.makedirs(temp_dir)
    subprocess.run(
        ["git", "clone", "--depth=1", "--branch", branch, repo_url, temp_dir],
        check=True,
    )
    print("Repository cloned successfully.")


def parse_flink(flink: List[Dict[str, str]]) -> List[FlinkJob]:
    if flink is None:
        return []

    ret = []

    for f in flink:
        ret.append(FlinkJob(f["mode"], f["name"], f["entry"]))
    return ret


def parse_jobs(file: str) -> List[Job]:
    jobs = []
    with open(file, "r") as f:
        conf: dict = json.load(f)
        flink_jobs = parse_flink(conf.get("flink", None))
        jobs.extend(flink_jobs)
    return jobs


def run_jobs(jobs: List[Job], modules: List[str]):
    for job in jobs:
        if job.typ in modules:
            job.run()


def build():
    # build lakesoul itself
    subprocess.run(
        ["mvn", "clean", "install", "-DskipTests"],
        check=True,
    )
    # build eeTests
    os.chdir(TMP_DIR + "/lakesoul-integ-test")
    subprocess.run(
        ["mvn", "clean", "install", "-DskipTests"],
        check=True,
    )
    os.chdir(TMP_DIR)


def main():
    # 解析命令行参数
    global TMP_DIR, GITHUB_URL, BRANCH, MODULES, VERSION
    parser = argparse.ArgumentParser(description="e2e test")
    parser.add_argument(
        "--module",
        default="flink",
        help="flink,spark,presto,all",
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="config file",
    )
    parser.add_argument(
        "--fresh",
        default=False,
        help="clone fresh code (default: False)",
    )
    parser.add_argument(
        "--code-dir",
        default=TMP_DIR,
        help="lakesoul repo dir",
    )
    args = parser.parse_args()

    try:
        TMP_DIR = args.code_dir
        print(TMP_DIR)
        # 拉取代码
        if args.fresh:
            clone_repo(GITHUB_URL, BRANCH, TMP_DIR)

        VERSION = get_version(TMP_DIR)
        print(VERSION)
        # now chdir
        cwd = os.getcwd()
        os.chdir(TMP_DIR)
        # build()
        modules = []
        if args.module == "all":
            modules = MODULES
        else:
            modules = list(args.module.split(","))

        os.chdir(cwd)
        jobs = parse_jobs(args.config)

        run_jobs(jobs, modules)

    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e}")
        print(f"End to end test failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
