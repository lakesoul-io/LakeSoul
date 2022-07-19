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

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.FileLifeCycleListener;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

public interface LakeSoulBucketFactory<IN, BucketID> extends Serializable {
  LakeSoulBucket<IN, BucketID> getNewBucket(int var1,
                                            BucketID var2,
                                            Path var3, long var4,
                                            BucketWriter<IN, BucketID> var6,
                                            LakeSoulRollingPolicyImpl var7,
                                            @Nullable FileLifeCycleListener<BucketID> var8,
                                            OutputFileConfig var9
  ) throws IOException;

  LakeSoulBucket<IN, BucketID> restoreBucket(int var1, long var2,
                                             BucketWriter<IN, BucketID> var4,
                                             LakeSoulRollingPolicyImpl var5,
                                             BucketState<BucketID> var6,
                                             @Nullable FileLifeCycleListener<BucketID> var7,
                                             OutputFileConfig var8
  ) throws IOException;
}
