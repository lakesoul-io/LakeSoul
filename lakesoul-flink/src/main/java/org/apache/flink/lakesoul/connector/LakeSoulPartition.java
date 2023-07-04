/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;
import java.util.List;

@PublicEvolving
public class LakeSoulPartition implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<Path> paths;

    private final List<String> partitionKeys;
    private final List<String> partitionValues;

    public LakeSoulPartition(List<Path> paths, List<String> partitionKeys, List<String> partitionValues) {
        this.paths = paths;
        this.partitionKeys = partitionKeys;
        this.partitionValues = partitionValues;
    }

    public List<Path> getPaths() {
        return paths;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }
}
