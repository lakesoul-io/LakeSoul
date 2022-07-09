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

package org.apache.flink.lakeSoul.sink.fileSystem.bulkFormat;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;

public final class DefaultRowFormatBuilder<IN>
    extends RowFormatBuilder<IN, String, DefaultRowFormatBuilder<IN>> {
  private static final long serialVersionUID = -8503344257202146718L;

  public DefaultRowFormatBuilder(
      Path basePath, Encoder<IN> encoder, BucketAssigner<IN, String> bucketAssigner) {
    super(basePath, encoder, bucketAssigner);
  }
}
