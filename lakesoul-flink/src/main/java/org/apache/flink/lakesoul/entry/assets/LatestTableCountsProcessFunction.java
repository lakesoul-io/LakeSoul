package org.apache.flink.lakesoul.entry.assets;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class LatestTableCountsProcessFunction extends ProcessWindowFunction<TableCounts, TableCounts, String, TimeWindow> {
    @Override
    public void process(String tableId,
                        ProcessWindowFunction<TableCounts, TableCounts, String, TimeWindow>.Context context,
                        Iterable<TableCounts> elements,
                        Collector<TableCounts> collector) throws Exception {
        //获取窗口里的最后一个元素
        TableCounts latestTableCounts = null;
        for (TableCounts element : elements) {
            latestTableCounts = element;
        }
        if (latestTableCounts != null) {
            collector.collect(latestTableCounts);
        }
    }
}
