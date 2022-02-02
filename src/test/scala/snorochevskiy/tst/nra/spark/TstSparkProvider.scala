package snorochevskiy.tst.nra.spark

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.sql.SparkSession

trait TstSparkProvider {

  implicit val sparkSession = SparkSession
    .builder()
    .appName("EU Statistical Regions")
    .master("local[*]")
    .getOrCreate()

  SedonaSQLRegistrator.registerAll(sparkSession)

}
