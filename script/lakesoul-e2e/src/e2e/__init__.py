# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0


import logging

import argparse


from .log import init_logger


DESC = """
    需要的环境变量
    AWS_BUCKET AWS_ACCESS_KEY_ID AWS_SECRET_KEY AWS_ENDPOINT 
    LAKESOUL_PG_URL LAKESOUL_PG_DRIVER LAKESOUL_PG_USERNAME LAKESOUL_PG_PASSWORD
"""


def main():
    """entry point"""
    parser = argparse.ArgumentParser(
        description=DESC, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dag_dir",
        type=str,
        default="./dags",
        help="input dag directory, default is './dags'",
    )
    parser.add_argument("--level", type=str, default="info", help="log level")
    parser.add_argument("--parallelism", type=int, default=4, help="worker nums")
    args = parser.parse_args()
    init_logger(args.level)
    run(args.dag_dir, args.parallelism)


def run(dag_dir, parallelism):
    try:
        from e2e.dag_runner import DAGRunner
        from e2e.models.dag import DagBag

        db = DagBag(dag_dir)
        runner = DAGRunner(parallelism, db)
        runner.run()
        logging.info("e2e finished")
    except KeyboardInterrupt as _:
        logging.info("interrupted by user")
    except Exception as e:
        logging.error(
            f"e2e failed by {e}",
        )
