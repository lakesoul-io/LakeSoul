package org.apache.flink.lakesoul.test;

import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.table.LakeSoulDynamicTableFactory;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.Factory;

import java.util.Optional;

public class MockLakeSoulCatalog {

    public static class TestLakeSoulCatalog extends LakeSoulCatalog {
        Factory testFactory;

        public void setTestFactory(Factory testFactory) {
            this.testFactory = testFactory;
        }

        @Override
        public Optional<Factory> getFactory() {
            return Optional.of(testFactory);
        }
    }

    public static class TestLakeSoulDynamicTableFactory extends LakeSoulDynamicTableFactory {
        DynamicTableSink testSink = null;

        DynamicTableSource testSource = null;

        public void setTestSink(DynamicTableSink testSink) {
            this.testSink = testSink;
        }

        public void setTestSource(DynamicTableSource testSource) {
            this.testSource = testSource;
        }

        @Override
        public DynamicTableSink createDynamicTableSink(Context context) {
            if (testSink == null) return super.createDynamicTableSink(context);
            return testSink;
        }

        @Override
        public DynamicTableSource createDynamicTableSource(Context context) {
            if (testSource == null) return super.createDynamicTableSource(context);
            return testSource;
        }
    }


}
