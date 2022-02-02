package snorochevskiy.tst.nra

import snorochevskiy.tst.nra.data.DataProvider
import snorochevskiy.tst.nra.flow.MonoxideFlow
import snorochevskiy.tst.nra.spark.SparkProvider
import snorochevskiy.tst.nra.syntax.CustomSyntaxOps._
import snorochevskiy.tst.nra.transform.AirQualityTransformer._
import snorochevskiy.tst.nra.transform.RegionsTransformer._

/**
 * This combined job demonstrates all the three functionality together.
 * See [[RegionsStatisticsJob]], [[FifthHigherCoJob]], [[TopReducedCOJob]]
 */
object CarbonMonoxideReportJob extends SparkProvider with DataProvider {

  def main(args: Array[String]): Unit = {
    val Array(euNutsPath, airReportPath) = args
    implicit val spark = sparkSession

    val regionsDf = loadRegionsData(euNutsPath) |> extractRegionsGeometryDf
    val carbonMonoxideDf = loadAirQualityData(airReportPath) |> extractCarbonMonoxide

    val minMaxAvgCo = MonoxideFlow.calcMinMaxAvgCOByNutsRegion(regionsDf, carbonMonoxideDf, "2019")
    minMaxAvgCo.show(1000, false)

    val fifthHigher = MonoxideFlow.fifthHigherRegionPerCountry(regionsDf, carbonMonoxideDf, "2019")
    fifthHigher.show(1000, false)

    val topReducedRegion = MonoxideFlow.top1RegionByMonoxideReduction(regionsDf, carbonMonoxideDf, "2018", "2019")
    println(topReducedRegion)
  }

}