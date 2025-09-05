# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import os
from pathlib import Path
import docker
from e2e.operators.python import PythonOperator

import tempfile

from e2e.utils import random_str
from git import Repo

from git.util import RemoteProgress

from tqdm import tqdm
import logging


class TqdmProgress(RemoteProgress):
    def __init__(self):
        super().__init__()
        self.pbar = None
        self.last_cur = 0
        self.last_max = 0

    def update(self, op_code, cur_count, max_count=None, message=""):
        # 初始化进度条
        if self.pbar is None and max_count:
            self.pbar = tqdm(total=max_count, desc="克隆进度", unit="个对象")  # type: ignore
            self.last_cur = 0
            self.last_max = max_count

        # 更新进度条
        if self.pbar and cur_count >= self.last_cur:  # type: ignore
            self.pbar.update(cur_count - self.last_cur)  # type: ignore
            self.last_cur = cur_count

        # 显示消息
        if message:
            self.pbar.set_postfix_str(message)  # type: ignore

        # 结束进度条
        if self.pbar and max_count and cur_count >= max_count:  # type: ignore
            self.pbar.close()
            self.pbar = None


def compile_main(repo_url: str, branch: str):
    def clone(repo_url, local_path, branch):
        # git clone
        logging.debug(f"clone {repo_name} {branch}")
        _ = Repo.clone_from(
            url=repo_url,
            to_path=local_path,
            branch=branch,
            multi_options=["--depth=1"],
            progress=TqdmProgress(),  # type: ignore
        )

    # compile with docker
    def compile(local_path, repo_name):
        client = docker.from_env()
        work = f"/workspace/{repo_name}"
        c = os.cpu_count() or 16

        uid = os.getuid()
        gid = os.getgid()
        extra_env = {}
        if os.environ.get("HTTPS_PROXY"):
            extra_env["HTTPS_PROXY"] = os.environ.get("HTTPS_PROXY")
        if os.environ.get("HTTP_PROXY"):
            extra_env["HTTP_PROXY"] = os.environ.get("HTTP_PROXY")
        container = client.containers.run(
            "dmetasoul-repo/e2e-build:0.1",
            auto_remove=True,
            user=f"{uid}:{gid}",
            detach=True,
            volumes=[f"{local_path}:{work}"],
            working_dir=work,
            command="mvn clean package -DskipTests",
            cpuset_cpus=f"0-{int((c - 1) / 2)}",
            network="host",
            environment=extra_env,
        )
        print(f"container name: {container.name}")
        for line in container.logs(stream=True):
            print(line.decode(), end="")

    # upload

    def upload(local_path: Path):
        # parse revision from xml
        import xml.etree.ElementTree as ET
        from e2e.variables import E2E_PATH_PREFIX, gmap
        from e2e.utils import oss_upload_files

        tree = ET.parse(f"{local_path}/pom.xml")
        root = tree.getroot()
        ns = {"ns": root.tag.split("}")[0].strip("{")}
        revision = root.find("ns:properties/ns:revision", ns)
        if revision is not None:
            version = revision.text
            gmap["version"] = version  # type: ignore
        else:
            raise RuntimeError("code parse revision")
        jars = list(local_path.rglob(f"lakesoul-*{version}.jar"))
        # upload to oss

        logging.info(f"upload {jars}")
        oss_upload_files(jars, dest_prefix=f"{E2E_PATH_PREFIX}/compile/")

    repo_name = repo_url.rstrip("/").split("/")[-1]
    if repo_name.endswith(".git"):
        repo_name = repo_name[:-4]
    local_path = Path(tempfile.gettempdir()) / random_str() / repo_name

    clone(repo_url, local_path, branch)
    compile(local_path, repo_name)
    upload(local_path)


class CompileOperator(PythonOperator):
    """
    build lakesoul by docker
    """

    def __init__(
        self,
        *,
        task_id: str | None = None,
        repo_url: str,
        branch="main",
        **kwargs,
    ) -> None:
        task_id = f"compile-{random_str()}" if task_id is None else task_id
        op_args = [repo_url, branch]
        super().__init__(
            task_id=task_id, python_callable=compile_main, op_args=op_args, **kwargs
        )
