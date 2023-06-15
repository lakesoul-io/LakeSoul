/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.sink.committer;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkGlobalCommittable;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Global Committer implementation for {@link LakeSoulMultiTablesSink}.
 *
 * <p>This global committer is responsible for taking staged part-files, i.e. part-files in "pending"
 * state, created by the {@link AbstractLakeSoulMultiTableSinkWriter}
 * and commit them globally, or put them in "finished" state and ready to be consumed by downstream
 * applications or systems.
 */
public class LakeSoulSinkGlobalCommitter implements GlobalCommitter<LakeSoulMultiTableSinkCommittable, LakeSoulMultiTableSinkGlobalCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulSinkGlobalCommitter.class);

    public LakeSoulSinkGlobalCommitter() {
    }


    @Override
    public void close() throws Exception {
        // Do nothing.
    }

    /**
     * Find out which global committables need to be retried when recovering from the failure.
     *
     * @param globalCommittables A list of {@link LakeSoulMultiTableSinkGlobalCommittable} for which we want to verify which
     *                           ones were successfully committed and which ones did not.
     * @return A list of {@link LakeSoulMultiTableSinkGlobalCommittable} that should be committed again.
     * @throws IOException if fail to filter the recovered committables.
     */
    @Override
    public List<LakeSoulMultiTableSinkGlobalCommittable> filterRecoveredCommittables(List<LakeSoulMultiTableSinkGlobalCommittable> globalCommittables) {
        return globalCommittables;
    }

    /**
     * Compute an aggregated committable from a list of committables.
     *
     * @param committables A list of {@link LakeSoulMultiTableSinkCommittable} to be combined into a {@link LakeSoulMultiTableSinkGlobalCommittable}.
     * @return an aggregated committable
     * @throws IOException if fail to combine the given committables.
     */
    @Override
    public LakeSoulMultiTableSinkGlobalCommittable combine(List<LakeSoulMultiTableSinkCommittable> committables) throws IOException {
        return LakeSoulMultiTableSinkGlobalCommittable.fromLakeSoulMultiTableSinkCommittable(committables);
    }

    /**
     * Commit the given list of {@link LakeSoulMultiTableSinkGlobalCommittable}.
     *
     * @param globalCommittables a list of {@link LakeSoulMultiTableSinkGlobalCommittable}.
     * @return A list of {@link LakeSoulMultiTableSinkGlobalCommittable} needed to re-commit, which is needed in case we
     * implement a "commit-with-retry" pattern.
     * @throws IOException if the commit operation fail and do not want to retry any more.
     */
    @Override
    public List<LakeSoulMultiTableSinkGlobalCommittable> commit(List<LakeSoulMultiTableSinkGlobalCommittable> globalCommittables) throws IOException, InterruptedException {
        LakeSoulMultiTableSinkGlobalCommittable globalCommittable = LakeSoulMultiTableSinkGlobalCommittable.fromLakeSoulMultiTableSinkGlobalCommittable(globalCommittables);
        LakeSoulSinkCommitter committer = LakeSoulSinkCommitter.INSTANCE;
        for (List<LakeSoulMultiTableSinkCommittable> committables : globalCommittable.getGroupedCommitables().values()) {
            committer.commit(committables);
        }
        return Collections.emptyList();
    }

    /**
     * Signals that there is no committable any more.
     *
     * @throws IOException if fail to handle this notification.
     */
    @Override
    public void endOfInput() throws IOException, InterruptedException {
        System.out.println("org.apache.flink.lakesoul.sink.committer.LakeSoulSinkGlobalCommitter.endOfInput");
    }
}
