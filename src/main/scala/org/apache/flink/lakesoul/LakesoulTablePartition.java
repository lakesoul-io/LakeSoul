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

import org.apache.flink.lakesoul.tools.FlinkUtil;
import org.apache.spark.sql.lakesoul.utils.PartitionInfo;
import org.apache.spark.sql.lakesoul.utils.DataFileInfo;

import java.util.ArrayList;
import java.util.List;
import scala.Array;
import scala.collection.JavaConverters;

public class LakesoulTablePartition {
    private String table_id;
    private String range_id;
    private String table_name;
    private String range_value = "-5";
    private long read_version = 1L;
    private long pre_write_version = 1L;
    private long last_update_time = -1L;
    private int delta_file_num = 0;
    private boolean be_compacted = false;

    public LakesoulTablePartition(String range_value, String table_id, String table_name) {
        this.range_value = range_value;
        this.range_id = FlinkUtil.generateRangeId();
        this.table_id = table_id;
        this.table_name = table_name;
    }

    public LakesoulTablePartition(String range_value) {
        this( range_value, "", "" );
    }

    public LakesoulTablePartition setDelta_file_num(int delta_file_num) {
        this.delta_file_num = delta_file_num;
        return this;
    }

    public LakesoulTablePartition setLast_update_time(long last_update_time) {
        this.last_update_time = last_update_time;
        return this;

    }

    public LakesoulTablePartition setTable_name(String table_name) {
        this.table_name = table_name;
        return this;

    }

    public LakesoulTablePartition setTable_id(String table_id) {
        this.table_id = table_id;
        return this;

    }

    public LakesoulTablePartition setBe_compacted(boolean be_compacted) {
        this.be_compacted = be_compacted;
        return this;

    }
    public String getRange_id(){
        return this.range_id;
    }



    //todo
//    public PartitionInfo toScalaPartitonInfo(List<LakesoulTableData> lakeTableDatas){
//        int size = lakeTableDatas.size();
//        DataFileInfo[] al = new DataFileInfo[size];
//        for(int i=0;i<size;i++){
//            al[i]=lakeTableDatas.get( i ).toScalaDataFileInfo();
//        }
//
//       return new PartitionInfo(this.table_id,this.range_id,this.table_name,this.range_value,this.read_version,this.pre_write_version,new DataFileInfo[0],al ,new DataFileInfo[0],this.last_update_time,this.delta_file_num,this.be_compacted);
//    }
}
