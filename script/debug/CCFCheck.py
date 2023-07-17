#!/usr/bin/python

# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

# -*- coding: UTF-8 -*-

import hashlib
import os
import sys
from pathlib import Path

import pandas as pd

DefaultPath = '/opt/spark/work-dir/result/ccf'
MD5 = 'md5'
Dcolumns = ['uuid', 'ip', 'hostname', 'requests', 'name', 'city', 'job', 'phonenum']


def getParquetFiles(dirpath):
    result = []
    if os.path.exists(dirpath):
        for filepath, dirnames, filenames in os.walk(dirpath):
            for filename in filenames:
                if filename.startswith("part") and filename.endswith(".parquet"):
                    result.append(dirpath + "/" + filename)
        return result
    else:
        print("File path is not existed")
        sys.exit()


def mergeParquets(dirpath):
    return sort_files_and_compare_md5(dirpath, MD5)


def sortParquetFile(dirpath):
    data_dir = Path(dirpath)
    finaldf = pd.concat(
        pd.read_parquet(parquet_file)
        for parquet_file in data_dir.glob('*.parquet')
    )
    md5str = sortDFAndMD5(finaldf)
    print(md5str)


def sort_files_and_compare_md5(dirpath, MD5):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .master("local[4]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(dirpath).coalesce(1).sort('pk')
    count = df.count()
    print(f'Records number read from {dirpath} is {count}')
    df.write.mode('overwrite').csv('/opt/spark/work-dir/result/final_single_file_csv', header=True)
    spark.stop()
    import glob
    files = glob.glob('/opt/spark/work-dir/result/final_single_file_csv/*.csv')
    if len(files) != 1:
        print('Wrong single csv ouput under /opt/spark/work-dir/result/final_single_file_csv/')
        sys.exit(1)

    def file_as_bytes(file):
        with file:
            return file.read()

    print(f'Compute sha256 checksum of final result file {files[0]}')
    checksum = hashlib.sha256(file_as_bytes(open(files[0], 'rb'))).hexdigest()
    print(f'checksum for {dirpath} is {checksum}')
    return checksum
    # if checksum == MD5:
    #     print('SHA256 checksum verification succeeded')
    #     sys.exit(0)
    # else:
    #     print('SHA256 checksum verification failed')
    #     sys.exit(2)


def sortDFAndMD5(df):
    res = df.sort_values(by='uuid')
    # res.to_csv("result",index=False)
    md5str = hashlib.sha256(res.to_csv(index=False).encode()).hexdigest()
    return md5str


if __name__ == "__main__":
    Action = "merge"
    if len(sys.argv) > 3:
        Action = sys.argv[1]
        ResultPath = sys.argv[2]
        ExpectedPath = sys.argv[3]
    else:
        print('usage: python CCFCheck.py /opt/path merge md5str')
    if Action.lower() == 'merge':
        resultMd5 = mergeParquets(ResultPath)
        # expectedMd5 = mergeParquets(ExpectedPath)
        print(resultMd5)
        print(resultMd5 == "906e6636e364cf2499bc090c7ddd1c92e5ddc77b35f2959a12670e3182c66992")

    # else:
    #     sortParquetFile(DefaultPath)
