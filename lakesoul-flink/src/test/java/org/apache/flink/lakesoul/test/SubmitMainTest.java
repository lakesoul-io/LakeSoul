package org.apache.flink.lakesoul.test;

import org.apache.flink.lakesoul.entry.sql.SubmitMain;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class SubmitMainTest {
    public static void main(String[] args) throws IOException, URISyntaxException {

        StringBuffer content = new StringBuffer();
        content.append("DROP table if exists SourceTable;\n");
        content.append("CREATE table SourceTable( f0 VARCHAR ) WITH ( 'connector' = 'datagen', 'rows-per-second'='1' );\n");
        content.append("DROP table if exists SinkTable;\n");
        content.append("CREATE table SinkTable( f0 VARCHAR ) WITH ( 'connector' = 'blackhole' );\n");
        content.append("INSERT INTO SinkTable SELECT f0 from SourceTable;\n");
        content.append("select * from SourceTable;\n");

        List<String> list = new ArrayList<>();
        list.add("--submit_type");
        list.add("flink");
        list.add("--job_type");
        list.add("stream");
        list.add("--language");
        list.add("--sql_path");
        list.add("~/Desktop/sublime-data/flink-sql-blackhole.sql");
        list.add("--flink.checkpoint");
        list.add("file:///tmp/flink/checkpoints");
        list.add("--flink.savepoint");
        list.add("file:///tmp/flink/savepoints");
        String[] testArgs = list.toArray(new String[0]);
        SubmitMain.main(testArgs);

    }
}
