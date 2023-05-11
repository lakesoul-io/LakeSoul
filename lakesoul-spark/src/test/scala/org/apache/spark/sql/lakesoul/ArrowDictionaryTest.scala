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

package org.apache.spark.sql.lakesoul

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.impl.UnionListWriter
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.types.pojo.DictionaryEncoding
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.collection.JavaConverters._

object ArrowDictionaryTest {
  def main(args: Array[String]): Unit = {
    val allocator: RootAllocator = new RootAllocator()

    val vector: ListVector = ListVector.empty("vector", allocator)
    val dictionaryVector1: ListVector = ListVector.empty("dict1", allocator)
    val dictionaryVector2: ListVector = ListVector.empty("dict2", allocator)

    val writer1: UnionListWriter = vector.getWriter
    writer1.allocate
    writer1.setValueCount(1)

    val dictWriter1: UnionListWriter = dictionaryVector1.getWriter
    dictWriter1.allocate
    dictWriter1.setValueCount(1)

    val dictWriter2: UnionListWriter = dictionaryVector2.getWriter
    dictWriter2.allocate
    dictWriter2.setValueCount(1)

    val dictionary1: Dictionary = new Dictionary(dictionaryVector1, new DictionaryEncoding(1L, false, None.orNull))
    val dictionary2: Dictionary = new Dictionary(dictionaryVector2, new DictionaryEncoding(1L, false, None.orNull))

    val provider = new DictionaryProvider.MapDictionaryProvider
    provider.put(dictionary1)
    provider.put(dictionary2)

    vector.clear()
    provider.getDictionaryIds.asScala.map(id => provider.lookup(id).getVector.clear())

    allocator.getAllocatedMemory shouldBe 0
  }

}
