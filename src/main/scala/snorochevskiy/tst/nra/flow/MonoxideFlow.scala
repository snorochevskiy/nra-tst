package snorochevskiy.tst.nra.flow

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, dense_rank, lag, max, min, typedLit}

object MonoxideFlow {

  /**
   * Calculates min, max and avg carbon monoxide pollution for each region.
   *
   * @param regionsDf region boundaries data
   * @param monoxideDf CO air pollution data
   * @param sparkSession
   * @return
   */
  def calcMinMaxAvgCOByNutsRegion(regionsDf: DataFrame, monoxideDf: DataFrame, year: String)
                                 (implicit sparkSession: SparkSession): DataFrame = {
    assert(year.matches("\\d{4}"))

    import sparkSession.implicits._

    regionsDf.createOrReplaceTempView("regionsDf")

    val monoxidePollution = monoxideDf
      .where($"ReportingYear" === typedLit(year))
    monoxidePollution.createOrReplaceTempView("monoxidePollution")

    val monoxideWithRegionsDf = sparkSession.sql(
      """
        | SELECT *
        | FROM regionsDf, monoxidePollution
        | WHERE ST_Contains(regionsDf.geometry, monoxidePollution.coord)
        |""".stripMargin)

    monoxideWithRegionsDf
      //.where($"ReportingYear" === typedLit("2019"))
      .groupBy("CountryOrTerritory", "NUTS_ID")
      .agg(
        avg("AQValue").as("avg_pollution"),
        min("AQValue").as("min_pollution"),
        max("AQValue").as("max_pollution")
      )
  }

  /**
   * For each country finds a region with the fifth highest average of Carbon monoxide (air) emission.
   *
   * @param regionsDf region boundaries data
   * @param monoxideDf CO air pollution data
   * @param sparkSession
   * @return
   */
  def fifthHigherRegionPerCountry(regionsDf: DataFrame, monoxideDf: DataFrame, year: String)
                                 (implicit sparkSession: SparkSession): DataFrame = {
    assert(year.matches("\\d{4}"))

    import sparkSession.implicits._

    regionsDf.createOrReplaceTempView("regionsDf")

    val monoxidePollution = monoxideDf
      .where($"ReportingYear" === typedLit(year))
    monoxidePollution.createOrReplaceTempView("monoxidePollution")

    val monoxideWithRegionsDf = sparkSession.sql(
      """
        | SELECT *
        | FROM regionsDf, monoxidePollution
        | WHERE ST_Contains(regionsDf.geometry, monoxidePollution.coord)
        |""".stripMargin)

    // There are regions that belong to multiple countries
    // (e.g. ES5 belongs to Spain but touches Andorra, CZ0 covers Czechia but touches Poland).
    // That's why if we want logically correct for each country find the EU statistical region with
    // the fifth highest average of Carbon monoxide (air), while calculating AVG pollution
    // for region, we need to actually group by CountryOrTerritory and NUTS_ID
    val avgCarbonMonoxideByCountryAndStation = monoxideWithRegionsDf
      //.where($"ReportingYear" === typedLit("2019"))
      .groupBy("CountryOrTerritory", "NUTS_ID")
      .agg(
        avg("AQValue").as("avg_pollution")
      )

    val avgPollutionByCountryWnd = Window.partitionBy("CountryOrTerritory").orderBy("avg_pollution")
    val fifthHigherRegionPerCountry = avgCarbonMonoxideByCountryAndStation
      .withColumn("rnk", dense_rank().over(avgPollutionByCountryWnd))
      .filter($"rnk" === typedLit(5))
      .drop("rnk")

    fifthHigherRegionPerCountry
  }

  def joinMetricsToRegions(regionsDf: DataFrame, metricsDf: DataFrame): DataFrame = {


    regionsDf.createOrReplaceTempView("regionsDf")
    metricsDf.createOrReplaceTempView("metricsDf")

    regionsDf.sparkSession.sql(
      """
        | SELECT *
        | FROM regionsDf, monoxide2019Pollution
        | WHERE ST_Contains(regionsDf.geometry, metricsDf.coord)
        |""".stripMargin)
  }

  def top1RegionByMonoxideReduction(regionsDf: DataFrame, monoxideDf: DataFrame, baseYear: String, nextYear: String)
                                   (implicit sparkSession: SparkSession): Option[String] = {
    assert(baseYear.matches("\\d{4}"))
    assert(nextYear.matches("\\d{4}"))

    import sparkSession.implicits._

    regionsDf.createOrReplaceTempView("regionsDf")

    val monoxide2018_2019Pollution = monoxideDf
      .where($"ReportingYear".isin("2018", "2019"))
    monoxide2018_2019Pollution.createOrReplaceTempView("monoxide2018_2019Pollution")

    val monoxide2018_2019WithRegionsDf = sparkSession.sql(
      """
        | SELECT *
        | FROM regionsDf, monoxide2018_2019Pollution
        | WHERE ST_Contains(regionsDf.geometry, monoxide2018_2019Pollution.coord)
        |""".stripMargin)

    val wnd = Window.partitionBy("NUTS_ID").orderBy("ReportingYear")

    val regionsByMonoxideReduction = monoxide2018_2019WithRegionsDf
      .groupBy("NUTS_ID", "ReportingYear")
      .agg(
        avg("AQValue").as("avg_pollution")
      )
      .withColumn("diff", $"avg_pollution" - lag("avg_pollution", 1).over(wnd))
      .where($"diff".isNotNull)
      .orderBy($"diff".asc)

    if (regionsByMonoxideReduction.isEmpty) None
    else Some(regionsByMonoxideReduction.head().getAs[String]("NUTS_ID"))
  }
}
