package org.apache.flink.lakesoul.metaData;


import org.apache.spark.sql.lakesoul.utils.DataFileInfo;

public class LakeSoulTableData {

    private String table_id;
    private String range_id;
    private  String range_Partition;
    private String file_path;
    private String file_exist_cols="";
    private boolean is_base_file=false;
    private long modification_time;
    private long size;
    private int write_version=-1;

    public LakeSoulTableData(String file_path,String file_exist_cols,boolean is_base_file,long modification_time,long size,int write_version,String range_partition) {
        this(file_path,modification_time,size,range_partition);
        this.is_base_file=is_base_file;
        this.write_version=write_version;
        this.file_exist_cols=file_exist_cols;

    }
    public LakeSoulTableData(String file_path,long modification_time,long size,String range_partition) {
        this.file_path=file_path;
        this.modification_time=modification_time;
        this.size=size;
        this.range_Partition=range_partition;
    }

    public LakeSoulTableData setTable_id(String table_id) {
        this.table_id = table_id;
        return this;
    }

    public LakeSoulTableData setRange_id(String range_id) {
        this.range_id = range_id;
        return this;
    }

    public void setIs_base_file(boolean is_base_file) {
        this.is_base_file = is_base_file;
    }

    public void setFile_exist_cols(String file_exist_cols) {
        this.file_exist_cols = file_exist_cols;
    }

    public DataFileInfo toScalaDataFileInfo(){
        //todo append需要修改
        return new DataFileInfo(this.range_Partition, this.file_path, "append", this.size, this.modification_time,this.file_exist_cols);
    }
}
