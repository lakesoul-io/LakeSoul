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

package org.apache.flink.lakesoul;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import java.io.IOException;
import java.util.List;
public interface PartitionTrigger {

    /** Add a pending partition. */
    void addPartition(String partition);

    /** Get committable partitions, and cleanup useless watermarks and partitions. */
    List<String> committablePartitions(long checkpointId) throws IOException;

    /** End input, return committable partitions and clear. */
    List<String> endInput();

    /** Snapshot state. */
    void snapshotState(long checkpointId, long watermark) throws Exception;

    static PartitionTrigger create(
            boolean isRestored,
            OperatorStateStore stateStore)
            throws Exception {
        return new PartitionTimeTrigger(
                isRestored, stateStore);

    }
}
