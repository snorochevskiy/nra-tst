package snorochevskiy.tst.nra

/**
 * You can use this class to run the job from IDE.
 * You cannot run [[CarbonMonoxideReportJob]] directly, because it is located in main scope,
 * and cannot use in runtime spark libraries, since they are "provided" dependencies.
 *
 * But in test scope, "provided" dependencies are accessible in runtime.
 */
object CarbonMonoxideReportJobIdeRun {

  def main(args: Array[String]): Unit = {

    System.setProperty("spark.master", "local")

    CarbonMonoxideReportJob.main(
      Array(
        "src/test/resources/nuts_rg_60m_2013_lvl_1.csv",
        "src/test/resources/eea_air_quality_report-subset.csv"
      )
    )

  }

}