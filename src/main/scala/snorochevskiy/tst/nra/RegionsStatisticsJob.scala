package snorochevskiy.tst.nra

import snorochevskiy.tst.nra.data.DataProvider
import snorochevskiy.tst.nra.flow.MonoxideFlow
import snorochevskiy.tst.nra.spark.SparkProvider
import snorochevskiy.tst.nra.syntax.CustomSyntaxOps._
import snorochevskiy.tst.nra.transform.AirQualityTransformer._
import snorochevskiy.tst.nra.transform.RegionsTransformer._

/**
 * For each EU statistical region ( NUTS_ID column in EU Statistical Regions boundaries dataset), calculate:
 * avg, min, max of AQValue ( from European Air quality report ) for "Carbon monoxide (air)" pollutant in year 2019
 */
object RegionsStatisticsJob extends SparkProvider with DataProvider {

  def main(args: Array[String]): Unit = {
    val Array(euNutsPath, airReportPath, output) = args
    implicit val spark = sparkSession

    val regionsDf = loadRegionsData(euNutsPath) |> extractRegionsGeometryDf
    val carbonMonoxideDf = loadAirQualityData(airReportPath) |> extractCarbonMonoxide

    val minMaxAvgCo = MonoxideFlow.fifthHigherRegionPerCountry(regionsDf, carbonMonoxideDf, "2019")

    minMaxAvgCo.write
      .option("header", true)
      .csv(output)
  }

}
