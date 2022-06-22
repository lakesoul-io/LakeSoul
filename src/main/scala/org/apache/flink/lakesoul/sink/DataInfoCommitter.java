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

package org.apache.flink.lakesoul.sink;

import com.alibaba.fastjson.JSON;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DataTypeUtil;
import com.dmetasoul.lakesoul.meta.MetaCommit;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import com.dmetasoul.lakesoul.meta.entity.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.metaData.LakeSoulTableData;
import org.apache.flink.lakesoul.sink.partition.PartitionTrigger;
import org.apache.flink.lakesoul.metaData.DataInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.flink.table.filesystem.FileSystemFactory;
import org.apache.spark.sql.types.StructField;

public class DataInfoCommitter extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<DataInfo, Void> {

    private static final long serialVersionUID = 1L;

    private final Configuration conf;

    private final Path locationPath;

    private final ObjectIdentifier tableIdentifier;

    private final List<String> partitionKeys;

    private final FileSystemFactory fsFactory;

    private transient PartitionTrigger trigger;

    private transient LakesoulTaskCheck taskTracker;

    private transient long currentWatermark;

    private DBManager dbManager;

    public DataInfoCommitter(
            Path locationPath,
            ObjectIdentifier tableIdentifier,
            List<String> partitionKeys,
            FileSystemFactory fsFactory,
            Configuration conf) {
        this.locationPath = locationPath;
        this.tableIdentifier = tableIdentifier;
        this.partitionKeys = partitionKeys;
        this.fsFactory = fsFactory;
        this.conf = conf;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        this.currentWatermark = Long.MIN_VALUE;
        this.dbManager=new DBManager();
        this.trigger =
                PartitionTrigger.create(
                        context.isRestored(),
                        context.getOperatorStateStore()
                     );

    }

    @Override
    public void processElement(StreamRecord<DataInfo> element) throws Exception {
        DataInfo message = element.getValue();
        System.out.println(message.getPartitions().size()+":::::datainfoPPPPPPPPP-======--((((((((((((((");
        for (String partition : message.getPartitions()) {
            trigger.addPartition(partition);
        }
        if (taskTracker == null) {
            taskTracker = new LakesoulTaskCheck(message.getNumberOfTasks());
        }
        boolean needCommit = taskTracker.add(message.getCheckpointId(), message.getTaskId());
        if (needCommit) {
            commitPartitions(message);
        }
    }

    private void commitPartitions(DataInfo element) throws Exception {

        long checkpointId = element.getCheckpointId ();
        List<String> partitions = checkpointId == Long.MAX_VALUE
                        ? trigger.endInput()
                        : trigger.committablePartitions(checkpointId);
        if (partitions.isEmpty()) {
            return;
        }

        String filenamePrefix = element.getTaskDataPath();

        TableInfo tableInfo = dbManager.getTableInfo(element.getTableName());
        MetaInfo metaInfo = new MetaInfo();
        metaInfo.setTableInfo(tableInfo);
        ArrayList<PartitionInfo> partitionLists = new ArrayList<>();
        ArrayList<DataCommitInfo> commitInfos = new ArrayList<>();


        for (String partition : partitions) {
            Path path = new Path(locationPath, partition);
            org.apache.flink.core.fs.FileStatus[] files =  path.getFileSystem().listStatus(path);
            for(FileStatus fs:files){
                if(!fs.isDir()){
                    long len=fs.getLen();
                    String onepath  = fs.getPath().toString();
                    if(onepath.contains( filenamePrefix )){
                        PartitionInfo partitionInfo = new PartitionInfo();
                        partitionInfo.setCommitOp("AppendCommit");
                        partitionInfo.setTableId(tableInfo.getTableId());
                        UUID uuid = UUID.randomUUID();
                        partitionInfo.setSnapshot(uuid);
                        partitionInfo.setPartitionDesc(partition);
                        partitionLists.add(partitionInfo);


                        DataFileOp dataFileOp = new DataFileOp();
                        dataFileOp.setPath(fs.getPath().toString());
                        dataFileOp.setSize(len);
                        dataFileOp.setFileOp("add");
                        //TODO
                        dataFileOp.setFileExistCols("user_id,dt,name");
                        DataCommitInfo dataCommitInfo = new DataCommitInfo();
                        dataCommitInfo.setCommitId(uuid);
                        dataCommitInfo.setPartitionDesc(partition);
                        dataCommitInfo.setCommitOp("AppendCommit");
                        dataCommitInfo.setFileOps(Collections.singletonList(dataFileOp));
                        dataCommitInfo.setTableId(tableInfo.getTableId());
                        dataCommitInfo.setTimestamp(System.currentTimeMillis());
                        commitInfos.add(dataCommitInfo);
                    }
                }
            }
        }
        HashMap<String, PartitionInfo> map = new HashMap<>();
        partitionLists.forEach(v->{
            String key = v.getTableId() + v.getPartitionDesc();
            if (map.containsKey(key)){
                PartitionInfo partitionInfo = map.get(key);
                List<UUID> snapshot = partitionInfo.getSnapshot();
                snapshot.addAll(v.getSnapshot());
                partitionInfo.setSnapshot(snapshot);
                map.put(key,partitionInfo);
            }else {
                map.put(key,v);
            }
        });
        ArrayList<PartitionInfo> distPartitionLists = new ArrayList<>();
        map.forEach((k,v)-> distPartitionLists.add(v));
        metaInfo.setListPartition(distPartitionLists);
        boolean appendCommit = dbManager.commitData(metaInfo, true, "AppendCommit");

        if(appendCommit){
            dbManager.batchCommitDataCommitInfo(commitInfos);
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        this.currentWatermark = mark.getTimestamp();
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        trigger.snapshotState(context.getCheckpointId(), currentWatermark);
    }


}