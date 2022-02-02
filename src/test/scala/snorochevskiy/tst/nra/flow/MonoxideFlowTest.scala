package snorochevskiy.tst.nra.flow

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import snorochevskiy.tst.nra.spark.TstSparkProvider

class MonoxideFlowTest extends AnyWordSpec with Matchers with TstSparkProvider {

  import sparkSession.implicits._

  "Monoxide statistics aggregator" must {
    "calculate min, max, avg pollution for it's region" in {

      val regionsDf = Seq(
        ("R1", "POLYGON ((10.0 10.0, 10.0 20.0, 20.0 20.0, 20.0 10.0, 10.0 10.0))"),
        ("R2", "POLYGON ((30.0 30.0, 30.0 40.0, 40.0 40.0, 40.0 30.0, 30.0 30.0))")
      ).toDF("NUTS_ID", "WKT")
        .selectExpr("NUTS_ID", "ST_GeomFromWKT(WKT) AS geometry")

      val monoxideDf = Seq(
        ("Country_1", "2019", 10.0, "POINT (11.0 11.0)"),
        ("Country_1", "2019", 20.0, "POINT (12.0 12.0)"),
        ("Country_1", "2019", 30.0, "POINT (13.0 13.0)"),
        ("Country_2", "2019", 1.0, "POINT (31.0 31.0)"),
        ("Country_2", "2019", 3.0, "POINT (32.0 32.0)"),
        ("Country_X", "2019", 99.0, "POINT (99.0 99.0)") // Doesn't belong to the region
      ).toDF("CountryOrTerritory", "ReportingYear", "AQValue", "point")
        .selectExpr("*", "ST_GeomFromWKT(point) AS coord")

      val res = MonoxideFlow.calcMinMaxAvgCOByNutsRegion(regionsDf, monoxideDf, "2019")
        .collect()

      res must not be empty
      res.length must be (2)

      val r1 = res.find(_.getAs[String]("NUTS_ID") == "R1").get
      r1.getAs[Double]("avg_pollution") mustBe 20.0
      r1.getAs[Double]("min_pollution") mustBe 10.0
      r1.getAs[Double]("max_pollution") mustBe 30.0

      val r2 = res.find(_.getAs[String]("NUTS_ID") == "R2").get
      r2.getAs[Double]("avg_pollution") mustBe 2.0
      r2.getAs[Double]("min_pollution") mustBe 1.0
      r2.getAs[Double]("max_pollution") mustBe 3.0
    }

    "calculate nothing for empty data" in {
      val regionsDf = Seq[(String, String)](
      ).toDF("NUTS_ID", "WKT")
        .selectExpr("NUTS_ID", "ST_GeomFromWKT(WKT) AS geometry")

      val monoxideDf = Seq[(String, String, Double, String)](
      ).toDF("CountryOrTerritory", "ReportingYear", "AQValue", "point")
        .selectExpr("*", "ST_GeomFromWKT(point) AS coord")
      val res = MonoxideFlow.calcMinMaxAvgCOByNutsRegion(regionsDf, monoxideDf, "2019")
        .collect()
      res.length mustBe 0
    }

    "find fifth higher region" in {

      val regionsDf = Seq(
        ("R1", "POLYGON ((10.0 10.0, 10.0 14.0, 14.0 14.0, 14.0 10.0, 10.0 10.0))"),
        ("R2", "POLYGON ((15.0 15.0, 15.0 19.0, 19.0 19.0, 19.0 15.0, 15.0 15.0))"),
        ("R3", "POLYGON ((20.0 20.0, 20.0 24.0, 24.0 24.0, 24.0 20.0, 20.0 20.0))"),
        ("R4", "POLYGON ((25.0 25.0, 25.0 29.0, 29.0 29.0, 29.0 25.0, 25.0 25.0))"),
        ("R5", "POLYGON ((30.0 30.0, 30.0 34.0, 34.0 34.0, 34.0 30.0, 30.0 30.0))")
      ).toDF("NUTS_ID", "WKT")
        .selectExpr("NUTS_ID", "ST_GeomFromWKT(WKT) AS geometry")

      val monoxideDf = Seq(
        ("Country_1", "2019", 10.0, "POINT (11.0 11.0)"),
        ("Country_1", "2019", 20.0, "POINT (16.0 16.0)"),
        ("Country_1", "2019", 30.0, "POINT (21.0 21.0)"),
        ("Country_1", "2019", 40.0, "POINT (26.0 26.0)"),
        ("Country_1", "2019", 60.0, "POINT (31.0 31.0)"),
        ("Country_1", "2019", 50.0, "POINT (99.0 99.0)") // Doesn't belong to any of regions
      ).toDF("CountryOrTerritory", "ReportingYear", "AQValue", "point")
        .selectExpr("*", "ST_GeomFromWKT(point) AS coord")

      val res = MonoxideFlow.fifthHigherRegionPerCountry(regionsDf, monoxideDf, "2019")
        .collect()

      res.length mustBe 1
      res(0).getAs[String]("CountryOrTerritory") mustBe "Country_1"
      res(0).getAs[String]("NUTS_ID") mustBe "R5"
    }

    "find the region top 1 by monoxide reduction" in {

      val regionsDf = Seq(
        ("R1", "POLYGON ((10.0 10.0, 10.0 14.0, 14.0 14.0, 14.0 10.0, 10.0 10.0))"),
        ("R2", "POLYGON ((15.0 15.0, 15.0 19.0, 19.0 19.0, 19.0 15.0, 15.0 15.0))"),
        ("R3", "POLYGON ((20.0 20.0, 20.0 24.0, 24.0 24.0, 24.0 20.0, 20.0 20.0))"),
      ).toDF("NUTS_ID", "WKT")
        .selectExpr("NUTS_ID", "ST_GeomFromWKT(WKT) AS geometry")

      val monoxideDf = Seq(
        ("Country_1", "2018", 10.0, "POINT (11.0 11.0)"),
        ("Country_1", "2019", 9.0, "POINT (11.0 11.0)"),
        ("Country_1", "2018", 20.0, "POINT (16.0 16.0)"),
        ("Country_1", "2019", 25.0, "POINT (16.0 16.0)"),
        ("Country_1", "2018", 30.0, "POINT (21.0 21.0)"),
        ("Country_1", "2019", 25.0, "POINT (21.0 21.0)"),
      ).toDF("CountryOrTerritory", "ReportingYear", "AQValue", "point")
        .selectExpr("*", "ST_GeomFromWKT(point) AS coord")

      val res = MonoxideFlow.top1RegionByMonoxideReduction(regionsDf, monoxideDf, "2018", "2019")
      res mustBe Some("R3")
    }
  }
}
