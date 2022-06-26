package com.dmetasoul.lakesoul.meta.entity;

public class DataFileOp {
  String path;
  String fileOp;
  long size;
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
