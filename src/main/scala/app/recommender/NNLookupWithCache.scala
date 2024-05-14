package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups (enhanced with cache)
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookupWithCache(lshIndex : LSHIndex) extends Serializable {
  var cache: Map[IndexedSeq[Int], List[(Int, String, List[String])]] = null

  var count: RDD[(IndexedSeq[Int], Iterable[(IndexedSeq[Int], List[String])], Int)] = null

  /**
   * The operation for building the cache
   *
   * @param sc Spark context for current application
   */
  def build(sc : SparkContext) = {
    val total = count.aggregate(0.0)((agg, elem) => agg + elem._3, (agg1, agg2) => agg1 + agg2)

    val occurrences = count.map(c => (c._1, c._2, c._3 * 100 / total)).filter(c => c._3 > 1).flatMap(c => c._2).distinct()

    val toCache =  lshIndex.lookup(occurrences)
    var newCache: Map[IndexedSeq[Int], List[(Int, String, List[String])]] = Map()

    if (!toCache.isEmpty()) {
      newCache = toCache.map(a => (a._1, a._3)).collect().toMap
    }

    cache = newCache
    sc.broadcast(cache)
  }

  /**
   * Testing operation: force a cache based on the given object
   *
   * @param ext A broadcast map that contains the objects to cache
   */
  def buildExternal(ext : Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]]) = {
    cache = ext.value
  }

  /**
   * Lookup operation on cache
   *
   * @param queries The RDD of keyword lists
   * @return The pair of two RDDs
   *         The first RDD corresponds to queries that result in cache hits and
   *         includes the LSH results
   *         The second RDD corresponds to queries that result in cache hits and
   *         need to be directed to LSH
   */
  def cacheLookup(queries: RDD[List[String]])
  : (RDD[(List[String], List[(Int, String, List[String])])], RDD[(IndexedSeq[Int], List[String])]) = {
    val queriesSignature = lshIndex.hash(queries)

    count = queriesSignature.groupBy(qs => qs._1).map(qs => (qs._1, qs._2, qs._2.size)).persist()

    if (cache == null) {
      (null, queriesSignature)
    } else {
      val hit = queriesSignature.filter(qs => cache contains qs._1).map(qs => {
        (qs._2, cache(qs._1))
      })

      (hit, queriesSignature.filter(qs => !(cache contains qs._1)))
    }
  }

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    val cres = cacheLookup(queries)
    val answers = lshIndex.lookup(cres._2)

    if (cres._1 == null || cres._1.isEmpty()) {
      if (answers.isEmpty()) {
        queries.map(q => (q, List()))
      } else {
        answers.map(a => (a._2, a._3))
      }
    } else {
      if (answers.isEmpty()) {
        cres._1
      } else {
        cres._1 ++ answers.map(a => (a._2, a._3))
      }
    }
  }
}
