package app.recommender

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    val queriesBucket = lshIndex.hash(queries)

    val answers = lshIndex.lookup(queriesBucket)

    if (answers.isEmpty()) {
      queries.map(q => (q, List()))
    } else {
      answers.map(a => (a._2, a._3))
    }
  }
}
