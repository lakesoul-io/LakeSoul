package org.apache.flink.lakesoul.connector;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.source.LakeSoulSplit;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.filesystem.PartitionFetcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Base class for table partition fetcher context. */
public abstract class LakeSoulPartitionFetcherContextBase<P> implements PartitionFetcher.Context<P> {


    List<Map<String, String>> remainingPartitions;
    protected TableId tableId;

    public TableId getTableId() {
        return tableId;
    }

    private List<String> partitionKeys;

    public LakeSoulPartitionFetcherContextBase(TableId tableId){
        this.tableId = tableId;
    }

    /**
     * open the resources of the Context, this method should first call before call other
     * methods.
     */
    @Override
    public void open() throws Exception {

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

        return new ArrayList<>();
    }

    /**
     * close the resources of the Context, this method should call when the context do not need
     * any more.
     */
    @Override
    public void close() throws Exception {

    }
}
