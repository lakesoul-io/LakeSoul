# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import os
import shutil
import sys
import tempfile
import unittest

from pyspark.sql import SparkSession


class LakeSoulTestCase(unittest.TestCase):
    """Test class base that sets up a correctly configured SparkSession for querying LakeSoul tables.
    """

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.warehouse_dir = tempfile.mkdtemp()
        # Configurations to speed up tests and reduce memory footprint
        self.spark = SparkSession.builder \
            .appName(class_name) \
            .master('local[4]') \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "5") \
            .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog") \
            .config("spark.sql.warehouse.dir", self.warehouse_dir) \
            .getOrCreate()
        self.sc = self.spark.sparkContext
        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self):
        self.sc.stop()
        shutil.rmtree(self.tempPath)
        if os.path.exists(self.warehouse_dir) and os.path.isdir(self.warehouse_dir):
            shutil.rmtree(self.warehouse_dir)
        sys.path = self._old_sys_path
