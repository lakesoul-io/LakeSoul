package org.apache.flink.lakesoul.test.flinkSource;

import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.api.Expressions.$;

public class ReadWithTableAPI extends AbstractTestBase {
    private final String BATCH_TYPE = "batch";

    @Test
    public void testLakesoulSourceSelectWhere() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        TestUtils.createLakeSoulSourceTableUser(createTableEnv);
        Table userInfo = createTableEnv.from("user_info");
        Table filter = userInfo.filter("order_id=3").select($("name"), $("score"));
        List<Row> results = CollectionUtil.iteratorToList(filter.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[Jack, 75]", "+I[Amy, 95]"});
    }

    @Test
    public void testLakesoulSourceSelectMultiRangeAndHash() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        TestUtils.createLakeSoulSourceMultiPartitionTable(createTableEnv);
        Table userInfo = createTableEnv.from("user_multi");
        Table filter = userInfo.filter("region='UK'").filter("score > 80")
                .select($("name"), $("score"), $("date"), $("region"));
        List<Row> results = CollectionUtil.iteratorToList(filter.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[Amy, 95, 1995-10-10, UK]"});
    }

    @Test
    public void testLakesoulSourceSelectJoin() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        TestUtils.createLakeSoulSourceTableUser(createTableEnv);
        TestUtils.createLakeSoulSourceTableOrder(createTableEnv);
        String testSelectJoin = "select ui.order_id,sum(oi.price) as total_price,count(*) as total " +
                "from user_info as ui inner join order_info as oi " +
                "on ui.order_id=oi.id group by ui.order_id having ui.order_id>2";
        Table userInfo = createTableEnv.from("user_info");
        Table orderInfo = createTableEnv.from("order_info");
        Table join = userInfo.join(orderInfo).where(($("order_id")).isEqual($("id"))).filter("order_id > 2")
                .select($("order_id"), $("name"), $("price"));
        Table result = join.groupBy("order_id")
                .select($("order_id"), $("price").sum().as("total_price"), $("order_id").count().as("total"));
        List<Row> results = CollectionUtil.iteratorToList(result.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[3, 30.7, 2]", "+I[4, 25.24, 1]", "+I[5, 15.04, 1]"});
    }
}
