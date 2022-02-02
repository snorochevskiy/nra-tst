package snorochevskiy.tst.nra

import snorochevskiy.tst.nra.data.DataProvider
import snorochevskiy.tst.nra.flow.MonoxideFlow
import snorochevskiy.tst.nra.spark.SparkProvider
import snorochevskiy.tst.nra.syntax.CustomSyntaxOps._
import snorochevskiy.tst.nra.transform.AirQualityTransformer._
import snorochevskiy.tst.nra.transform.RegionsTransformer._

/**
 * Find which EU statistical region had the biggest reduction of average CO emissions for year 2019
 * compared to the previous year - 2018.
 */
object TopReducedCOJob extends SparkProvider with DataProvider {

  def main(args: Array[String]): Unit = {
    val Array(euNutsPath, airReportPath) = args
    implicit val spark = sparkSession

    val regionsDf = loadRegionsData(euNutsPath) |> extractRegionsGeometryDf
    val carbonMonoxideDf = loadAirQualityData(airReportPath) |> extractCarbonMonoxide

    val topReducedRegion = MonoxideFlow.top1RegionByMonoxideReduction(regionsDf, carbonMonoxideDf, "2018", "2019")

    topReducedRegion match {
      case Some(region) =>
        println(s"Highest CO reduction in: $region")
      case None =>
        System.err.println("Cannot find a region with highest CO reduction")
        System.exit(1)
    }
  }

}
