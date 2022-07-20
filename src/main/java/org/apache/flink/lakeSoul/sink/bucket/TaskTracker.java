/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakeSoul.sink.bucket;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

public class TaskTracker {
  private final int taskCount;

  private final TreeMap<Long , Set<Integer>> notifyTask = new TreeMap<>();

  public TaskTracker(int bucketCount) {
    this.taskCount =bucketCount;
  }

  public boolean addCompleteTask(long checkpointId, int bucket){
    Set<Integer> currentCkpBucket = notifyTask.computeIfAbsent(checkpointId, v -> new HashSet<>());
    currentCkpBucket.add(bucket);
    if(taskCount ==currentCkpBucket.size()){
      notifyTask.headMap(checkpointId,true).clear();
      return true;
    }
      return false;
  }
}
