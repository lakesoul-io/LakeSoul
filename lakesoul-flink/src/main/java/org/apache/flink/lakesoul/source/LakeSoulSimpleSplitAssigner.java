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

package org.apache.flink.lakesoul.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class LakeSoulSimpleSplitAssigner {
    private final ArrayList<LakeSoulSplit> splits;

    public LakeSoulSimpleSplitAssigner(Collection<LakeSoulSplit> splits) {
        this.splits = new ArrayList<>(splits);
    }
    public LakeSoulSimpleSplitAssigner() {
        this.splits = new ArrayList<>();
    }

    public Optional<LakeSoulSplit> getNext() {
        final int size = splits.size();
        return size == 0 ? Optional.empty() : Optional.of(splits.remove(size - 1));
    }

    public void addSplits(Collection<LakeSoulSplit> newSplits) {
        splits.addAll(newSplits);
    }

    public List<LakeSoulSplit> remainingSplits() {
        return splits;
    }

    @Override
    public String toString() {
        return "LakeSoulSimpleSplitAssigner " + splits;
    }
}
