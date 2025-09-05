# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0


import os
import random
import string
from pathlib import Path
import subprocess
import opendal
from typing import List

from e2e.variables import BUCKET, ACCESS_KEY_ID, E2E_PATH_PREFIX, SECRET_KEY, END_POINT

import asyncio


# oss
OPERATOR = opendal.AsyncOperator(
    "s3",
    bucket=BUCKET,
    region="auto",
    access_key_id=ACCESS_KEY_ID,
    secret_access_key=SECRET_KEY,
    endpoint=END_POINT,
)


def oss_upload_files(files: List[Path], dest_prefix):
    async def inner(files, dest_prefix):
        for file in files:
            with open(file, "rb") as data:
                await OPERATOR.write(f"{dest_prefix}/{file.name}", data.read())

    return asyncio.run(inner(files, dest_prefix))


def oss_delete_jars(jars: List[Path], dest_prefix):
    async def inner(jars, dest_prefix):
        for jar in jars:
            await OPERATOR.delete(f"{dest_prefix}/{jar.name}")

    return asyncio.run(inner(jars, dest_prefix))


def oss_list_prefix_dir(prefix: str) -> List[str]:
    async def inner(prefix):
        if not prefix.endswith("/"):
            prefix += "/"
        ret = []
        iter = await OPERATOR.list(prefix)
        async for e in iter:
            ret.append(e.path)
        return ret

    return asyncio.run(inner(prefix))


def oss_delete_prefix(prefix: str):
    async def inner(prefix: str):
        await OPERATOR.remove_all(prefix)

    return asyncio.run(inner(prefix))


def oss_read(path, encoding="utf-8") -> str:
    async def inner(path, encoding):
        content = await OPERATOR.read(path)
        return content.decode(encoding=encoding)

    return asyncio.run(inner(path, encoding))


def oss_write(path, data):
    async def inner(path, data):
        await OPERATOR.write(path, data)

    return asyncio.run(inner(path, data))


def oss_create_dir(prefix: str, fresh):
    async def inner(prefix):
        if not prefix.endswith("/"):
            prefix += "/"
        if fresh:
            await OPERATOR.remove_all(prefix)
        await OPERATOR.create_dir(prefix)

    return asyncio.run(inner(prefix))


def get_jars_by_name(name: str):
    lst = oss_list_prefix_dir(f"{E2E_PATH_PREFIX}/compile/")
    ret = []
    for p in lst:
        if name in p:
            ret.append(f"s3://{BUCKET}/{p}")
    assert len(ret) == 1
    return ret[0]


# normal


def random_str(length=8):
    chars = string.ascii_letters + string.digits  # 包含大小写字母和数字
    return "".join(random.choices(chars, k=length))


def fill_skeleton(output: str):
    with open(
        "java-skeleton/src/main/java/com/dmetasoul/e2e/Main.java",
        "w",
        encoding="utf-8",
    ) as f:
        f.write(output)


def build_skeleton():
    old_dir = os.getcwd()
    os.chdir("java-skeleton")
    args = ["mvn", "clean", "package"]
    subprocess.run(args, check=True,capture_output=True)
    os.chdir(old_dir)


def upload(task_id, rename: str | None = None) -> str:
    from pathlib import Path

    folder = Path("java-skeleton/target")
    prefix = "e2e"

    files = [
        f
        for f in folder.iterdir()
        if f.is_file() and f.name.startswith(prefix) and f.name.endswith(".jar")
    ]
    prefix = f"{E2E_PATH_PREFIX}/{task_id}/jars"
    assert len(files) == 1
    if rename:
        new_name=files[0].with_name(rename)
        files[0].rename(new_name)
        files[0] = new_name

    oss_upload_files(files, prefix)
    return f"s3://{BUCKET}/{prefix}/{files[0].name}"
