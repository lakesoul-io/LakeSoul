/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.model

import org.apache.spark.ml.lakesoul.scanns.Types.{BandedHashes, Item, ItemId, ItemIdDistancePair}
import org.apache.spark.ml.lakesoul.scanns.distance.Distance
import org.apache.spark.ml.lakesoul.scanns.lsh.HashFunction
import org.apache.spark.ml.lakesoul.scanns.params.{HasSeed, LSHNNSParams}
import org.apache.spark.ml.lakesoul.scanns.utils.TopNQueue
import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
  * Abstract class with most of the implementation of an LSH based model. The implementing classes only need
  * implement [[getBandedHashes()]] and define the distance metric
  */
abstract class LSHNearestNeighborSearchModel[T <: LSHNearestNeighborSearchModel[T]]
  extends NearestNeighborModel[LSHNearestNeighborSearchModel[T]] with LSHNNSParams with HasSeed {

  /* Metric that will be used to compute pair-wise distances */
  val distance: Distance

  private[scanns] def getHashFunctions: Array[_ <: HashFunction]

  /**
    * The input here is an iterator over a tuple of two [[mutable.ArrayBuffer]]. Both arrays contain item ids and
    * the pre-condition is that item ids in the union of the two arrays mapped to the same hash bucket under a given
    * signature size. The first array contains the item ids for whom we are seeking candidates and the second array
    * contains items from the candidate pool
    *
    * Since all were mapped to the same "hash-bucket", the items in the second array are potential candidates for all
    * items in the first array. We want at most [[numNearestNeighbors]] candidates for each item with the lowest
    * distance
    *
    * This iterator produces the candidates in the form of tuples of [[Long]] and an [[Iterator(Long, Double)]]. The
    * first element of the tuple is the item id for whom candidates are required. The iterator contains candidate
    * item ids along with the distance values
    *
    * The primary purpose of this structure is to produce the results one at a time rather than materialize all of them
    * in memory. Materializing all together leads to GC overhead errors.
    *
    * @param bucketsIt Iterator of the tuples of the two item id arrays
    * @param itemVectors For computation of jaccard similarity, we need to know the attributes of the item
    *                         given the item id. This map contains a mapping from the item id [[Long]] to
    *                         [[Set]] of attributes
    * @param numNearestNeighbors Maximum number of candidates required for each item
    */
  private[model] class NearestNeighborIterator(bucketsIt: Iterator[Array[mutable.ArrayBuffer[ItemId]]],
                                itemVectors: mutable.Map[ItemId, Vector],
                                numNearestNeighbors: Int) extends Iterator[(ItemId, Iterator[ItemIdDistancePair])]
    with Serializable {

    // this will be the next element that the iterator returns on a call to next()
    private var nextResult: Option[(ItemId, Iterator[ItemIdDistancePair])] = None

    // this is the current tuple in the bucketsIt iterator that is being scanned
    private var currentTuple = if (bucketsIt.hasNext) Some(bucketsIt.next) else None

    // this is the index in the first array of currentTuple which is being scanned
    private var currentIndex = 0

    private def populateNext(): Unit = {
      var done = false
      while (currentTuple.isDefined && !done) {
        currentTuple match {
          case Some(x) =>
            while (currentIndex < x(0).size && !done) {
              val queue = new TopNQueue(numNearestNeighbors)
              x(1).filter(_ != x(0)(currentIndex))
                .map(c => (c, distance.compute(itemVectors(c), itemVectors(x(0)(currentIndex)))))
                .foreach(queue.enqueue(_))
              if (queue.nonEmpty()) {
                nextResult = Some((x(0)(currentIndex), queue.iterator()))
                done = true
              }
              currentIndex += 1
            }
            if (currentIndex == x(0).size) {
              currentIndex = 0
              currentTuple = if (bucketsIt.hasNext) Some(bucketsIt.next) else None
            }
          case _ =>
        }
      }
      if (currentTuple.isEmpty && !done) {
        nextResult = None
      }
    }

    populateNext()

    override def hasNext: Boolean = nextResult.isDefined

    override def next(): (ItemId, Iterator[ItemIdDistancePair]) = {
      if (hasNext) {
        val ret = nextResult.get
        populateNext()
        return ret
      }
      null
    }
  }

  /**
    * Firstly, note that this hash has nothing to do with the hashes of locality sensitive hashing. Think of this more
    * in the flavor of the feature hashing "trick". See https://en.wikipedia.org/wiki/Feature_hashing
    *
    * This method is used so that we need not maintain the meaty (hashes, index) tuple that will be shuffled around
    * during joins. It is important to note that we want the hash based on the content of the array and the integer
    * value itself rather than the object identity of the tuple. This is why we cannot use scala object.hashCode
    * directly but have to rely on [[MurmurHash3]]
    *
    * 37 is just a prime number.
    * Refer https://stackoverflow.com/questions/113511/best-implementation-for-hashcode-method/113600#113600
    *
    * @param hashesWithBucketIndex hash bucket to be "integer-ized"
    * @return integer hash of the bucket
    */
  private def getHashCode(hashesWithBucketIndex: (Array[Int], Int)): Int = {
    37 * (37 * 1 + MurmurHash3.arrayHash(hashesWithBucketIndex._1)) + hashesWithBucketIndex._2
  }

  /**
    * Given an input vector, get the banded hashes by hashing it using the hash functions. A band itself performs
    * AND-amplification but the idea of multiple bands is to allow OR-amplification
    *
    * @param x input vector
    * @return banded hashes
    */
  def getBandedHashes(x: Vector): BandedHashes

  /**
    * Apply [[getBandedHashes()]] to each row in the dataset
    *
    * @param data input data
    * @return row containing the id, vector representation and the banded hashes
    */
  def transform(data: RDD[Item]): RDD[(ItemId, (Vector, BandedHashes))] = {
    data.mapValues(x => (x, getBandedHashes(x)))
  }

  /**
    * Explode the data into multiple rows, one for each bucket that gets created. This allows us to frame the problem
    * of figuring out matching buckets as that of a join on the bucket id, created using [[getHashCode()]]. Note that the
    * item id and the vector representation gets replicated as many times as the number of bands in the output
    *
    * @param transformedData input data transformed using [[transform()]]
    * @return data containing bucket id and (item id, vector) tuples
    */
  def explodeData(transformedData: RDD[(ItemId, (Vector, BandedHashes))]): RDD[(Int, Item)] = {
    transformedData.flatMap { case (id, (vector, bandedHashes)) =>
      bandedHashes.zipWithIndex.map(getHashCode).map{ x => (x, (id, vector)) }
    }
  }

  /**
    * This is kept for debugging purposes. The main memory bottleneck for this implementation comes from the eventual
    * sizes of itemVectors map and, more importantly, the number of items falling within a bucket. This is because,
    * within a bucket, we have to perform a brute force all-pairs computation so that if the bucket becomes too large,
    * either w.r.t srcItems or candidatePoolItems, that can make this very slow.
    *
    * This method is used to log the stats about each partition's data structure and their sizes
    *
    * @param partitionId id of the partition
    * @param itemVectors map of item ids to their vector representation
    * @param hashBuckets bucket id mapped to items that fall in that bucket
    */
  private def logStats(
                        partitionId: Int,
                        itemVectors: mutable.Map[ItemId, Vector],
                        hashBuckets: mutable.Map[Int, Array[mutable.ArrayBuffer[ItemId]]]): Unit = {
    System.out.println(s"Partition id [$partitionId] stats:")
    System.out.println(s"Size of item vectors map: [${itemVectors.size}]")
    System.out.println(s"Number of hash buckets: [${hashBuckets.size}]")
    hashBuckets.foreach{ case (bucketId, items) =>
      System.out.println(s"Bucket id [$bucketId] has [${items(0).size}] src items and [${items(1).size}] candidates")
    }
  }

  /**
    * This method updates the given hashBuckets map using the provided item iterator.
    *
    * @param itemIt iterator over bucket ids and the contained items with their vector representations
    * @param itemVectors item vector map that will be updated to keep a map of item ids to vectors
    * @param hashBuckets map of bucket ids to items that fall in that bucket
    * @param shouldReservoirSample boolean to decide whether for large buckets, reservoir sampling should be used for
    *                              populating them or simply the first encountered items till the limit is reached
    * @param isCandidatePoolIt The updating of the hashBuckets map is identical for src and candidate items except for
    *                          a small difference. This boolean is helpful in avoiding copying of code
    */
  private def updateHashBuckets(itemIt: Iterator[(Int, (ItemId, Vector))],
                        itemVectors: mutable.Map[ItemId, Vector],
                        hashBuckets: mutable.Map[Int, Array[mutable.ArrayBuffer[ItemId]]],
                        shouldReservoirSample: Boolean = false,
                        isCandidatePoolIt: Boolean): Unit = {
    // Maintain number of elements seen by both arrays, to perform streaming reservoir sampling. If a bucket
    // has more than `bucketLimit` items, we use reservoir sampling to decide whether to keep the incoming items
    // or not so that we have a uniform sample of `bucketLimit` items at the end of processing the entire stream
    val numElementsSeen = mutable.Map[Int, Int]()

    val selector = if (isCandidatePoolIt) 1 else 0

    itemIt.foreach { case (h, (id, vector)) =>
      if (hashBuckets.contains(h)) {
        if (hashBuckets(h)(selector).size == $(bucketLimit)) {
          if (shouldReservoirSample) {
            if (numElementsSeen.contains(h)) {
              numElementsSeen(h) += 1
            } else {
              numElementsSeen.put(h, $(bucketLimit) + 1)
            }
            val index = Random.nextInt(numElementsSeen(h))
            if (index < $(bucketLimit)) {
              // Unfortunately, we can't remove the item vector for `hashBuckets(h)(selector)(index)` from itemVectors
              // map because some other bucket might still contain that item
              hashBuckets(h)(selector)(index) = id
              if (! itemVectors.contains(id)) {
                itemVectors.put(id, vector)
              }
            }
          }
          // if reservoir sampling is disabled, we just ignore everything else
        } else {
          hashBuckets(h)(selector) += id
          if (! itemVectors.contains(id)) {
            itemVectors.put(id, vector)
          }
        }
      } else {
        if (! isCandidatePoolIt) {
          hashBuckets.put(h, Array(mutable.ArrayBuffer[ItemId](id), mutable.ArrayBuffer[ItemId]()))
          if (! itemVectors.contains(id)) {
            itemVectors.put(id, vector)
          }
        }
      }
    }
  }

  /**
    * Get k nearest neighbors to all items in srcItems dataset from the candidatePool dataset
    *
    * @param srcItems Items for which neighbors are to be found
    * @param candidatePool Items which are potential candidates
    * @param k number of nearest neighbors needed
    * @return nearest neighbors in the form (srcItemId, candidateItemId, distance)
    */
  override def getAllNearestNeighbors(srcItems: RDD[Item], candidatePool: RDD[Item], k: Int):
  RDD[(ItemId, ItemId, Double)] = {
    val hashPartitioner = new HashPartitioner($(joinParallelism))
    val srcItemsExploded = explodeData(transform(srcItems)).partitionBy(hashPartitioner)
    val candidatePoolExploded = if (srcItems.id == candidatePool.id) {
      srcItemsExploded
    } else {
      explodeData(transform(candidatePool)).partitionBy(hashPartitioner)
    }
    srcItemsExploded.zipPartitions(candidatePoolExploded) {
        case (srcIt, candidateIt) => {
          val itemVectors = mutable.Map[ItemId, Vector]()
          val hashBuckets = mutable.Map[Int, Array[mutable.ArrayBuffer[ItemId]]]()

          updateHashBuckets(
            srcIt,
            itemVectors,
            hashBuckets,
            shouldReservoirSample = $(shouldSampleBuckets),
            isCandidatePoolIt = false)
          updateHashBuckets(
            candidateIt,
            itemVectors,
            hashBuckets,
            shouldReservoirSample = $(shouldSampleBuckets),
            isCandidatePoolIt = true)

          // TODO Start using loggers to log useful info
          // logStats(TaskContext.getPartitionId(), itemVectors, hashBuckets)
          new NearestNeighborIterator(hashBuckets.valuesIterator, itemVectors, k)
        }
      }
      .groupByKey()
      .mapValues { candidateIter =>
        val topN = new TopNQueue(k)
        candidateIter.flatten.foreach(topN.enqueue(_))
        topN.iterator()
      }
      .flatMap{ x => x._2.map(z => (x._1, z._1, z._2)) }
      .repartition($(numOutputPartitions))
  }

  /**
    * Get k nearest neighbors for all input items from within itself
    *
    * @param items Set of items
    * @param k number of nearest neighbors needed
    * @return nearest neighbors in the form (srcItemId, candidateItemId, distance)
    */
  def getSelfAllNearestNeighbors(items: RDD[Item], k: Int): RDD[(ItemId, ItemId, Double)] = {
    // TODO Not efficient since if srcItems = candidatePool, we don't need to maintain 2 array buffers in hashBuckets map
    getAllNearestNeighbors(items, items, k)
  }

  /**
    * Get k nearest neighbors to the query vector from the given items
    *
    * @param key query vector
    * @param items items to be searched for nearest neighbors
    * @param k number of nearest neighbors needed
    * @return array of (itemId, distance) tuples
    */
  override def getNearestNeighbors(key: Vector, items: RDD[Item], k: Int): Array[ItemIdDistancePair] = {
    val hashes = items.sparkContext.broadcast(
      getBandedHashes(key).zipWithIndex.map(getHashCode).toSet)
    explodeData(transform(items))
      .filter{ case (hash, _) => hashes.value.contains(hash) }
      .map { case (_, (id, vector)) =>
        (id, distance.compute(key.toSparse, vector))
      }
      .takeOrdered(k)(Ordering.by[ItemIdDistancePair, Double](_._2))
  }
}
