package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    sc.textFile("src/main/resources" + path).map(rdd => rdd.split('|')).map(cols => {
      if (cols.length == 4) {
        (cols(0).toInt, cols(1).toInt, None, cols(2).toDouble, cols(3).toInt)
      } else {
        (cols(0).toInt, cols(1).toInt, Option(cols(2).toDouble), cols(3).toDouble, cols(4).toInt)
      }
    }).persist()
  }
}
