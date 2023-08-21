// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;

public abstract class AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.test.util.AbstractTestBase.class);

    private static final int DEFAULT_PARALLELISM = 16;

    private static Configuration getConfig() {
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        config.set(ExecutionCheckpointingOptions.TOLERABLE_FAILURE_NUMBER, 5);
        config.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(3));
        config.setString("state.backend.type", "hashmap");
        config.setString("state.checkpoint.dir", getTempDirUri("/flinkchk"));
        return config;
    }

    @ClassRule
    public static MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfig())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .build());

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @After
    public final void cleanupRunningJobs() throws Exception {
        if (!miniClusterResource.getMiniCluster().isRunning()) {
            // do nothing if the MiniCluster is not running
            LOG.warn("Mini cluster is not running after the test!");
            return;
        }

        for (JobStatusMessage path : miniClusterResource.getClusterClient().listJobs().get()) {
            if (!path.getJobState().isTerminalState()) {
                try {
                    miniClusterResource.getClusterClient().cancel(path.getJobId()).get();
                } catch (Exception ignored) {
                    // ignore exceptions when cancelling dangling jobs
                }
            }
        }
    }

    /*
     * @path: a subdir name under temp dir, e.g. /lakesoul_table
     * @return: file://PLATFORM_TMP_DIR/path
     */
    public static String getTempDirUri(String path) {
        String tmp = System.getProperty("java.io.tmpdir");
        Path tmpPath = new Path(tmp, path);
        File tmpDirFile = new File(tmpPath.toString());
        tmpDirFile.deleteOnExit();
        return tmpPath.makeQualified(LocalFileSystem.getSharedInstance()).toUri().toString();
    }
}
