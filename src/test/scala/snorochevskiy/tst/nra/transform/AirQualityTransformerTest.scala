package snorochevskiy.tst.nra.transform

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import snorochevskiy.tst.nra.spark.TstSparkProvider

class AirQualityTransformerTest extends AnyWordSpec with Matchers with TstSparkProvider {

  import sparkSession.implicits._

  "Raw air quality data transformer" must {

    "filter out not valid records and transform the geometry" in {
      val raw = Seq(
        ("Country_1", "2019", 51.7, 19.4, "Carbon monoxide (air)", "Valid", "Verified", 91.0, 5.0),
        ("Country_2", "2019", 51.7, 19.4, "Carbon monoxide (air)", "Not valid", "Verified", 91.0, 5.0),
        ("Country_3", "2019", 51.7, 19.4, "Carbon monoxide (air)", "Valid", "Not verified", 91.0, 5.0),
        ("Country_4", "2019", 51.7, 19.4, "Carbon monoxide (air)", "Valid", "Verified", 89.0, 5.0)
      ).toDF("CountryOrTerritory", "ReportingYear", "SamplingPoint_Latitude", "SamplingPoint_Longitude",
        "Pollutant", "Validity", "Verification", "DataCoverage", "AQValue")

      val res = AirQualityTransformer.extractCarbonMonoxide(raw)
        .collect()

      res.length mustBe 1
      res(0).getAs[Double]("CountryOrTerritory") mustBe "Country_1"
      res(0).getAs[org.locationtech.jts.geom.Point]("coord").toString mustBe "POINT (19.4 51.7)"
    }
  }
}
