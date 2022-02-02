package snorochevskiy.tst.nra.spark

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.sql.SparkSession

trait SparkProvider {
  lazy val sparkSession = SparkSession
    .builder()
    .appName("EU Statistical Regions")
    .getOrCreate()

  SedonaSQLRegistrator.registerAll(sparkSession)
}
