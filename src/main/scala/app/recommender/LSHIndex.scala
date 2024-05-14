package app.recommender

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {
  private val minhash = new MinHash(seed)

  var buckets: RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = null

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets()
    : RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {

    val input = data.map(d => d._3)
    val hashBuckets = hash(input).keyBy(hd => hd._2)
    val hashedData = hashBuckets.join(data.keyBy(d => d._3)).map(hd => (hd._2._1._1, hd._2._2)).distinct().groupByKey().map(hd => (hd._1, hd._2.toList))
    hashedData
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {
    val answers = queries.leftOuterJoin(getBuckets())

    if (answers.isEmpty()) {
      queries.map(q => (q._1, q._2, List()))

    } else {
      answers.map(a => {
        a._2._2 match {
          case Some(x) => (a._1, a._2._1, x)
          case None => (a._1, a._2._1, List())
        }
      })
    }
  }
}
