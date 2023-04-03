/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.meta.entity;

/**
 * Singleton Data File information
 */
public class DataFileOp {
  /**
   * Physical qualified path of a parquet file
   */
  String path;

  /**
   * Set of {add, delete}, which define the specific operation of this file
   * add: indicates that the parquet file is newly added
   * delete: indicates that the parquet file has been deleted
   */
  String fileOp;

  /**
   * File size of byte-unit
   */
  long size;

  /**
   * Columns included with this parquet file, which should be equivalent of the meta of parquet file
   */
  String fileExistCols;

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getFileOp() {
    return fileOp;
  }

  public void setFileOp(String fileOp) {
    this.fileOp = fileOp;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public String getFileExistCols() {
    return fileExistCols;
  }

  public void setFileExistCols(String fileExistCols) {
    this.fileExistCols = fileExistCols;
  }
}
