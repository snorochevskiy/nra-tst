package snorochevskiy.tst.nra

import snorochevskiy.tst.nra.data.DataProvider
import snorochevskiy.tst.nra.flow.MonoxideFlow
import snorochevskiy.tst.nra.spark.SparkProvider
import snorochevskiy.tst.nra.syntax.CustomSyntaxOps._
import snorochevskiy.tst.nra.transform.AirQualityTransformer._
import snorochevskiy.tst.nra.transform.RegionsTransformer._

/**
 * For each country, Find the EU statistical region with the fifth highest average of
 * Carbon monoxide (air) emissions for year 2019.
 */
object FifthHigherCoJob extends SparkProvider with DataProvider {

  def main(args: Array[String]): Unit = {
    val Array(euNutsPath, airReportPath, output) = args
    implicit val spark = sparkSession

    val regionsDf = loadRegionsData(euNutsPath) |> extractRegionsGeometryDf
    val carbonMonoxideDf = loadAirQualityData(airReportPath) |> extractCarbonMonoxide

    val fifthHigher = MonoxideFlow.fifthHigherRegionPerCountry(regionsDf, carbonMonoxideDf, "2019")

    fifthHigher.write
      .option("header", true)
      .csv(output)
  }

}