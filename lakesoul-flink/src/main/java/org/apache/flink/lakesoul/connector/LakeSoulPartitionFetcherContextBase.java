package org.apache.flink.lakesoul.connector;

import com.dmetasoul.lakesoul.meta.*;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.source.LakeSoulSplit;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.filesystem.PartitionFetcher;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.table.filesystem.DefaultPartTimeExtractor.toMills;

/** Base class for table partition fetcher context. */
public abstract class LakeSoulPartitionFetcherContextBase<P> implements PartitionFetcher.Context<P> {


    List<Map<String, String>> remainingPartitions;
    protected TableId tableId;

    public TableId getTableId() {
        return tableId;
    }

    private List<String> partitionKeys;

    protected transient DBManager dbManager;

    public LakeSoulPartitionFetcherContextBase(TableId tableId){
        this.tableId = tableId;
    }

    /**
     * open the resources of the Context, this method should first call before call other
     * methods.
     */
    @Override
    public void open() throws Exception {
        dbManager = new DBManager();
    }

    private DataFileInfo[] getTargetDataFileInfo(TableInfo tableInfo) throws Exception {
        return FlinkUtil.getTargetDataFileInfo(tableInfo, null);
    }


    /**
     * Get list that contains partition with comparable object.
     *
     * <p>For 'create-time' and 'partition-time',the comparable object type is Long which
     * represents time in milliseconds, for 'partition-name', the comparable object type is
     * String which represents the partition names string.
     */
    @Override
    public List<ComparablePartitionValue> getComparablePartitionValueList() throws Exception {
        List<ComparablePartitionValue> partitionValueList = new ArrayList<>();
        TableInfo tableInfo = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(), tableId.schema());
        List<String> partitionInfos = this.dbManager.getAllPartitionInfo(tableInfo.getTableId()).stream()
                .map(partitionInfo -> partitionInfo.getPartitionDesc()).collect(Collectors.toList());
        for (String partitionInfo: partitionInfos) {
            partitionValueList.add(getComparablePartitionByName(partitionInfo));
        }
        return partitionValueList;
//        return new ArrayList<>();
    }

    private ComparablePartitionValue<List<String>, String> getComparablePartitionByName(
            String partitionName) {
        return new ComparablePartitionValue<List<String>, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public List<String> getPartitionValue() {
                return new ArrayList<String>(){{add(partitionName);}};
            }

            @Override
            public String getComparator() {
                // order by partition name in alphabetical order
                // partition name format: pt_year=2020,pt_month=10,pt_day=14
                return partitionName;
            }
        };
    }

    /**
     * close the resources of the Context, this method should call when the context do not need
     * any more.
     */
    @Override
    public void close() throws Exception {
    }
}
