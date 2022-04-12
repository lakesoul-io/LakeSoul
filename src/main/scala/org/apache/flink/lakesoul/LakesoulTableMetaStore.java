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
import com.dmetasoul.lakesoul.Newmeta.NewSnapshotManagement;
import com.dmetasoul.lakesoul.meta.CommitType$;
import com.dmetasoul.lakesoul.Newmeta.NewMetaCommit;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.spark.sql.lakesoul.utils.MetaInfo;
import org.apache.spark.sql.lakesoul.utils.PartitionInfo;
import org.apache.spark.sql.lakesoul.utils.TableInfo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import com.dmetasoul.lakesoul.meta.CommitType;
import org.apache.spark.sql.lakesoul.utils.CommitOptions;
import scala.None;

public class LakesoulTableMetaStore implements Serializable {

    private static final long serialVersionUID = 1L;
    private ObjectIdentifier obj;
    private Map<String,List<LakesoulTableData>> partitionWithDatas;
    private Map<String,LakesoulTablePartition> partitions;
    private String table_id;
    private String table_name;
    private  TableInfo tableInfo;
    LakesoulTableMetaStore(ObjectIdentifier obj) {
        this.obj=obj;
        this.partitions=new HashMap<>(  );
        partitionWithDatas=new HashMap<>(  );
        this.table_name=obj.toObjectPath().getFullName();
    }
    public void withPartition(String partitionName){
        if(obj != null && table_id == null){
            getTableInfo(this.table_name);
        }
        if(!partitions.containsKey( partitionName )){
            LakesoulTablePartition ltp=new LakesoulTablePartition(partitionName,table_id,table_name );
            partitions.put( partitionName,ltp );
        }
    }
    public void withData(String partitionName,LakesoulTableData ltd){
        withPartition( partitionName );
        if(!partitionWithDatas.containsKey( partitionName )){
            ArrayList<LakesoulTableData> al = new ArrayList<>();
            partitionWithDatas.put( partitionName,al );
        }
        ltd.setRange_id( partitions.get( partitionName ).getRange_id() ).setTable_id( table_id );
        partitionWithDatas.get( partitionName ).add(ltd);

    }
    public void getTableInfo(String tableName){
         this.tableInfo = NewSnapshotManagement.apply(tableName).getTableInfoOnly();
         this.table_id=tableInfo.table_id();
    }
    private PartitionInfo[] generatePartitionArray(){
        int partionsize = partitions.keySet().size();
        PartitionInfo[] parInfos = new PartitionInfo[partionsize];
        int i=0;
        for(Map.Entry<String, LakesoulTablePartition> item : partitions.entrySet()){
            String partitinkey = item.getKey();
            parInfos[i]=item.getValue().toScalaPartitonInfo( partitionWithDatas.get( partitinkey ) );
            i++;
        }
        return parInfos;
    }

    public void commit(){
        PartitionInfo[] partitionInfos = generatePartitionArray();
        NewMetaCommit.doMetaCommit( new MetaInfo(tableInfo,partitionInfos, CommitType$.MODULE$.apply("delta"),"","",-1L ),false , new CommitOptions(scala.Option.apply( null ), scala.Option.apply( null )),0);
    }

}

