/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.utils

import org.apache.spark.ml.lakesoul.scanns.Types.{ItemId, ItemIdDistancePair}

import scala.collection.mutable

/**
  * This is a simple wrapper around the scala [[mutable.PriorityQueue]] that allows it to only hold a fixed number of
  * elements. By default, [[mutable.PriorityQueue]] behaves as a max-priority queue i.e as a max heap. [[TopNQueue]]
  * can be used to get smallest-n elements in a streaming fashion.
  *
  * We also deduplicate the contents based on the first value of the tuple ([[ItemId]] id).
  *
  * @param maxCapacity max number of elements the queue will hold
  */
class TopNQueue(maxCapacity: Int) extends Serializable {

  val priorityQ: mutable.PriorityQueue[ItemIdDistancePair] =
    mutable.PriorityQueue[ItemIdDistancePair]()(Ordering.by[ItemIdDistancePair, Double](_._2))
  val elements: mutable.HashSet[ItemId] = mutable.HashSet[ItemId]() // for deduplication

  /**
    * Enqueue elements in the queue
    * @param elems The elements to enqueue
    */
  def enqueue(elems: ItemIdDistancePair*): Unit = {
    elems.foreach { x =>
      if (!elements.contains(x._1)) {
        if (priorityQ.size < maxCapacity) {
          priorityQ.enqueue(x)
          elements.add(x._1)
        } else {
          if (priorityQ.head._2 > x._2) {
            elements.remove(priorityQ.dequeue()._1)
            priorityQ.enqueue(x)
            elements.add(x._1)
          }
        }
      }
    }
  }

  def nonEmpty(): Boolean = priorityQ.nonEmpty

  def iterator(): Iterator[ItemIdDistancePair] = priorityQ.reverseIterator
}
