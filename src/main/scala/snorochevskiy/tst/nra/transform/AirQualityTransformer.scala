package snorochevskiy.tst.nra.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.typedLit

object AirQualityTransformer {

  /**
   * Takes raw air quality data and extracts valid and verified carbon monoxide data.
   * 
   * @param airQualityDf
   * @return
   */
  def extractCarbonMonoxide(airQualityDf: DataFrame): DataFrame = {
    import airQualityDf.sparkSession.implicits._

    // The Unit is same everywhere: mg.m-3
    val validAirQualityDf = airQualityDf
      .where($"DataCoverage" >= typedLit(90.0))
      .where($"Validity" === typedLit("Valid") && $"Verification" === typedLit("Verified"))
      .selectExpr(
        "ST_Point(CAST(SamplingPoint_Longitude AS Decimal(24,20)), CAST(SamplingPoint_Latitude AS Decimal(24,20))) AS coord",
        "CountryOrTerritory", "ReportingYear", "Pollutant", "AQValue"
      )

    validAirQualityDf
      .where($"Pollutant" === typedLit("Carbon monoxide (air)"))
      // https://goodforgas.com/what-could-cause-a-negative-reading-on-a-co-sensor/
      // Carbon monoxide cannot be negative and zero values also do not look as valid data,
      // though the Validity column contains "Valid"
      .where($"AQValue" > typedLit(0.0))
  }



}
