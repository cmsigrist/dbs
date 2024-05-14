package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class TitlesLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    sc.textFile("src/main/resources" + path).map(rdd => rdd.split('|')).map(cols => (cols(0).toInt, cols(1), cols.drop(2).toList)).persist()
  }
}
