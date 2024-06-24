package org.apache.flink.lakesoul.source.arrow;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.source.*;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LakeSoulArrowSource extends LakeSoulSource<LakeSoulArrowWrapper> {

    private final byte[] encodedTableInfo;

    public static LakeSoulArrowSource create(
            String tableNamespace,
            String tableName,
            Configuration conf
    ) throws IOException {
        TableId tableId = new TableId(LakeSoulCatalog.CATALOG_NAME, tableNamespace, tableName);
        TableInfo tableInfo = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableName, tableNamespace);
        RowType tableRowType = ArrowUtils.fromArrowSchema(Schema.fromJSON(tableInfo.getTableSchema()));
        DBUtil.TablePartitionKeys tablePartitionKeys = DBUtil.parseTableInfoPartitions(tableInfo.getPartitions());
        return new LakeSoulArrowSource(
                tableInfo,
                tableId,
                conf.toMap(),
                tableRowType,
                tablePartitionKeys.primaryKeys,
                tablePartitionKeys.rangeKeys
        );
    }

    LakeSoulArrowSource(
            TableInfo tableInfo,
            TableId tableId,
            Map<String, String> optionParams,
            RowType tableRowType,
            List<String> pkColumns,
            List<String> partitionColumns
    ) {
        super(
                tableId,
                tableRowType,
                tableRowType,
                tableRowType,
                false,
                pkColumns,
                partitionColumns,
                optionParams,
                null,
                null,
                null
        );
        this.encodedTableInfo = tableInfo.toByteArray();
    }


    /**
     * Creates a new reader to read data from the splits it gets assigned. The reader starts fresh
     * and does not have any state to resume.
     *
     * @param readerContext The {@link SourceReaderContext context} for the source reader.
     * @return A new SourceReader.
     * @throws Exception The implementor is free to forward all exceptions directly. Exceptions
     *                   thrown from this method cause task failure/recovery.
     */
    @Override
    public SourceReader<LakeSoulArrowWrapper, LakeSoulPartitionSplit> createReader(SourceReaderContext readerContext) throws Exception {
        Configuration conf = Configuration.fromMap(optionParams);
        conf.addAll(readerContext.getConfiguration());
        return new LakeSoulSourceReader(
                () -> new LakeSoulArrowSplitReader(
                        encodedTableInfo,
                        conf,
                        this.tableRowType,
                        this.projectedRowType,
                        this.rowTypeWithPk,
                        this.pkColumns,
                        this.isBounded,
                        this.optionParams.getOrDefault(LakeSoulSinkOptions.CDC_CHANGE_COLUMN, ""),
                        this.partitionColumns,
                        this.pushedFilter),
                new LakeSoulRecordEmitter(),
                readerContext.getConfiguration(),
                readerContext);
    }
}
