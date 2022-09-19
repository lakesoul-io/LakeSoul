package org.apache.flink.lakeSoul.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

public class MiniClusterResource {
    private static final int DEFAULT_TM_NUM = 1;
    private static final int DEFAULT_PARALLELISM = 4;

    public static final Configuration DISABLE_CLASSLOADER_CHECK_CONFIG =
            new Configuration()
                    // disable classloader check as Avro may cache class/object in the serializers.
                    .set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

    private MiniClusterResource() {}

    /**
     * It will start a mini cluster with classloader.check-leaked-classloader=false, so that we won't
     * break the unit tests because of the class loader leak issue. In our iceberg integration tests,
     * there're some that will assert the results after finished the flink jobs, so actually we may
     * access the class loader that has been closed by the flink task managers if we enable the switch
     * classloader.check-leaked-classloader by default.
     */
    public static MiniClusterWithClientResource createWithClassloaderCheckDisabled() {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberTaskManagers(DEFAULT_TM_NUM)
                        .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                        .setConfiguration(DISABLE_CLASSLOADER_CHECK_CONFIG)
                        .build());
    }
}
