// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.facebook.presto.lakesoul.handle.LakeSoulTableLayoutHandle;
import com.facebook.presto.lakesoul.pojo.Path;
import com.facebook.presto.lakesoul.util.PrestoUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LakeSoulSplitManager implements ConnectorSplitManager {

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
                                          ConnectorSession session,
                                          ConnectorTableLayoutHandle layout,
                                          SplitSchedulingContext splitSchedulingContext) {

        LakeSoulTableLayoutHandle tableLayout = (LakeSoulTableLayoutHandle) layout;
        String tid = tableLayout.getTableHandle().getId();
        List<FilterPredicate> parFilters = tableLayout.getParFilters();
        List partitions = new ArrayList<String>();
        for (FilterPredicate fp : parFilters) {
            if (fp instanceof Operators.Eq) {
                Operators.Column column = ((Operators.Eq) fp).getColumn();
                if (column instanceof Operators.IntColumn || column instanceof Operators.LongColumn) {
                    partitions.add(column.getColumnPath().toDotString() + "=" + ((Operators.Eq) fp).getValue());
                } else if (column instanceof Operators.BinaryColumn) {
                    Binary value = (Binary) ((Operators.Eq) fp).getValue();
                    partitions.add(column.getColumnPath().toDotString() + "=" + value.toStringUsingUTF8());
                } else {
                    break;
                }

            } else {
                partitions.clear();
                break;
            }
        }
        DataFileInfo[] dfinfos = DataOperation.getTableDataInfo(tid, JavaConverters.asScalaBuffer(partitions).toList());
        ArrayList<ConnectorSplit> splits = new ArrayList<>(16);
        Map<String, Map<Integer, List<Path>>>
                splitByRangeAndHashPartition =
                PrestoUtil.splitDataInfosToRangeAndHashPartition(tid, dfinfos);
        for (Map.Entry<String, Map<Integer, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
            for (Map.Entry<Integer, List<Path>> split : entry.getValue().entrySet()) {
                splits.add(new LakeSoulSplit(tableLayout, split.getValue()));
            }
        }
        return new LakeSoulSplitSource(splits);
    }

}
