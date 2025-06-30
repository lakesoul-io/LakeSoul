# SPDX-FileCopyrightText: LakeSoul Contributors
# 
# SPDX-License-Identifier: Apache-2.0

from main import s3_upload_jars,s3_delete_jars,s3_delete_dir,E2E_DATA_DIR
import unittest

class TestMainUtils(unittest.TestCase):
    def test_s3(self):
        # remove 
        s3_delete_jars()
        s3_upload_jars()
        s3_delete_dir(E2E_DATA_DIR)
        
        

if __name__ == "__main__":
    unittest.main()