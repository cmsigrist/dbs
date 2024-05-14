package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc : SparkContext) extends Serializable {

  var state = null
  var titles: RDD[(Int, (Int, String, List[String]))] = null
  var initState: RDD[((Int, String, List[String]), Option[(Int, Int, Option[Double], Double, Int)])] = null
  var aggregatedState: RDD[((Int, String, List[String]), (Double, Int))]= null


  // only update in aggregate, give new list, check if has an option than do count -1 and sum - Option and then count + 1 and sum + New
  def aggregateAvg(toAggregate: RDD[((Int, String, List[String]), Option[(Int, Int, Option[Double], Double, Int)])]): RDD[((Int, String, List[String]), (Double, Int))]  = {
    val aggregatedResults = toAggregate.aggregateByKey((0.0, 0))((acc, elem) => {
      var a = 0.0
      var length = 0
      elem match {
        case Some(e) =>
          a += e._4
          e._3 match {
            case Some(x) =>
              length += 0
              a -= x
            case None =>
              length += 1
          }
          (acc._1 + a, acc._2 + length)
        case None => acc
      }
    }, (acc1, acc2) => {
      (acc1._1 + acc2._1, acc1._2 + acc2._2)
    }).map(agg => {
      val sum: Double = agg._2._1
      val count = agg._2._2

      if (sum == 0 && count == 0) {
        (agg._1, (0.0, 0))
      } else {
        (agg._1, (sum, count))
      }
    })

    aggregatedResults
  }

  def incrementalAggregate(newRatings: RDD[((Int, String, List[String]), Option[(Int, Int, Option[Double], Double, Int)])]): RDD[((Int, String, List[String]), (Double, Int))]  = {
    val incrementalAgg = aggregateAvg(newRatings)

    aggregatedState.leftOuterJoin(incrementalAgg).map(j => {
      j._2._2 match {
        case Some(x) => (j._1, (j._2._1._1 + x._1, j._2._1._2 + x._2))
        case None => (j._1, (j._2._1._1, j._2._1._2))
      }
    })
  }

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings : RDD[(Int, Int, Option[Double], Double, Int)],
            title : RDD[(Int, String, List[String])]
          ) : Unit = {

    val ratingMap = ratings.keyBy(r => r._2)
    val titleMap = title.keyBy(t => t._1)

    titles = titleMap.persist()
    initState = titleMap.leftOuterJoin(ratingMap).map(rt => rt._2).persist()
    aggregatedState = aggregateAvg(initState).persist()

  }


  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult() : RDD[(String, Double)] = {
    aggregatedState.map(agg => {
      val sum = agg._2._1
      val count = agg._2._2
      if (count == 0) {
        (agg._1._2, 0.0)
      } else {
        (agg._1._2, sum /count)
      }
    })
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords : List[String]) : Double = {
    val aggKeywords = aggregatedState.aggregate((0.0, 0, 0))((acc, elem) => {
      if (keywords.forall(elem._1._3.contains)) {
        var rated = 0
        var notRated = 0

        if (elem._2._2 != 0) {
          rated += 1
        } else {
          notRated += 1
        }
        if (elem._2._2 == 0) {
          (acc._1 + 0.0, acc._2 + rated, acc._3 + notRated)
        } else {
          (acc._1 + (elem._2._1/elem._2._2), acc._2 + rated, acc._3 + notRated)
        }
      } else {
        acc
      }
    }, (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3))

    if (aggKeywords._2 == 0 && aggKeywords._3 == 0) {
      -1.0
    } else {
      // There is a movie contains the keywords
      if (aggKeywords._2 != 0) {
        aggKeywords._1 / aggKeywords._2
      } else {
        // All movies containing the keywords have no rating
        0.0
      }
    }
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]) : Unit = {
    val updatedDelta = delta_

    initState = initState.map(init => {
      var updatedInit = init
      delta_.foreach(d => {
        init._2 match {
          case Some(x) =>
            // user rated this movie
            if (d._1 == x._1 && d._2 == x._2) {
              val index = delta_.indexOf(d)
              updatedDelta.update(index,(d._1, d._2, Option(x._4), d._4, d._5))
              updatedInit = (init._1, None)
            } else {
              updatedInit = init
            }
          case None => updatedInit = init
        }
      })
      updatedInit
    })

    val delta = sc.makeRDD(updatedDelta).keyBy(d => d._2)
    val deltaTitle = titles.join(delta).map(rt => (rt._2._1, Option(rt._2._2)))
    initState = (initState ++ deltaTitle).distinct().persist()

    aggregatedState = incrementalAggregate(deltaTitle).persist()
  }
}
