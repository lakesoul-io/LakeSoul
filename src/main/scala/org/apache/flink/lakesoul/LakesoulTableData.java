/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul;

import com.dmetasoul.lakesoul.Newmeta.MetaCommon;
import org.apache.spark.sql.lakesoul.utils.DataFileInfo;

public class LakesoulTableData {

    private String table_id;
    private String range_id;
    private  String range_Partition;
    private String file_path;
    private String file_exist_cols="";
    private boolean is_base_file=false;
    private long modification_time;
    private long size;
    private int write_version=-1;

    public LakesoulTableData(String file_path,String file_exist_cols,boolean is_base_file,long modification_time,long size,int write_version,String range_partition) {
        this(file_path,modification_time,size,range_partition);
        this.is_base_file=is_base_file;
        this.write_version=write_version;
        this.file_exist_cols=file_exist_cols;

    }
    public LakesoulTableData(String file_path,long modification_time,long size,String range_partition) {
        this.file_path=file_path;
        this.modification_time=modification_time;
        this.size=size;
        this.range_Partition=range_partition;
    }

    public LakesoulTableData setTable_id(String table_id) {
        this.table_id = table_id;
        return this;
    }

    public LakesoulTableData setRange_id(String range_id) {
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
        return new DataFileInfo(this.file_path, MetaCommon.getPartitionMapFromKey( this.range_Partition ),this.size,this.modification_time,this.write_version,this.is_base_file,this.file_exist_cols);
    }
}
