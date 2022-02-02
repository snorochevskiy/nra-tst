package snorochevskiy.tst.nra.data

import org.apache.spark.sql.DataFrame
import snorochevskiy.tst.nra.spark.SparkProvider

/**
 * Here we could have an abstraction layer that allows to load data in different formats,
 * depending on e.g. extension.
 */
trait DataProvider { this: SparkProvider =>

  def loadAirQualityData(src: String): DataFrame =
    sparkSession.read
      .format("csv")
      .option("header", "true")
      .load(src)

  def loadRegionsData(src: String): DataFrame =
    sparkSession.read
      .format("csv")
      .option("header", "true")
      .load(src)
}
