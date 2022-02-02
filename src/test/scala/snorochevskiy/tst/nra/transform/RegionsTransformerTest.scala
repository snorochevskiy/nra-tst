package snorochevskiy.tst.nra.transform

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import snorochevskiy.tst.nra.spark.TstSparkProvider

class RegionsTransformerTest extends AnyWordSpec with Matchers with TstSparkProvider {

  import sparkSession.implicits._

  "Raw EU Statistical Regions boundaries LEVEL 1 data transformer" must {
    "convert WKT polygons to geometry" in {

      val raw = Seq(
        ("R1", "POLYGON ((10.0 10.0, 10.0 14.0, 14.0 14.0, 14.0 10.0, 10.0 10.0))")
      )toDF("NUTS_ID", "WKT")

      val res = RegionsTransformer.extractRegionsGeometryDf(raw)
        .collect()

      res.length mustBe 1
      res(0).getAs[org.locationtech.jts.geom.Polygon]("geometry").getCoordinates.length mustBe 5
    }
  }
}
