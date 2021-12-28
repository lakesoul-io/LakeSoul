#
# Copyright [2022] [DMetaSoul Team]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os
import shutil
import subprocess
from os import path


def test(root_dir):
    # Run all of the test under test/python directory, each of them
    # has main entry point to execute, which is python's unittest testing
    # framework.
    python_root_dir = path.join(root_dir, "python")
    test_dir = path.join(python_root_dir, path.join("lakesoul", "tests"))
    test_files = [os.path.join(test_dir, f) for f in os.listdir(test_dir)
                  if os.path.isfile(os.path.join(test_dir, f)) and
                  f.endswith(".py") and not f.startswith("_")]
    extra_class_path = path.join(python_root_dir, path.join("lakesoul", "testing"))

    for test_file in test_files:
        try:
            cmd = ["spark-submit",
                   "--driver-class-path=%s" % extra_class_path,
                   "--conf", "spark.dmetasoul.lakesoul.meta.database.name=test_lakesoul_meta",
                   test_file]
            print("Running tests in %s\n=============" % test_file)
            print("Command: %s" % str(cmd))
            run_cmd(cmd, stream_output=True)
        except:
            print("Failed tests in %s" % (test_file))
            raise


def delete_if_exists(path):
    # if path exists, delete it.
    if os.path.exists(path):
        shutil.rmtree(path)
        print("Deleted %s " % path)


def run_cmd(cmd, throw_on_error=True, env=None, stream_output=False, **kwargs):
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, **kwargs)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % (exit_code))
        return exit_code
    else:
        child = subprocess.Popen(
            cmd,
            env=cmd_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs)
        (stdout, stderr) = child.communicate()
        exit_code = child.wait()
        if throw_on_error and exit_code is not 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                (exit_code, stdout, stderr))
        return exit_code, stdout, stderr


if __name__ == "__main__":
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # package = prepare(root_dir)
    # run_python_style_checks(root_dir)
    test(root_dir)
