package snorochevskiy.tst.nra.transform

import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.sql.DataFrame

object RegionsTransformer {

  /**
   * Expects as input the European NUTS L1 boundaries data
   * taken from https://datahub.io/core/geo-nuts-administrative-boundaries
   * and converted from GeoJSON into WKT.
   * This WKT regional boundaries is parsed to geometry structures using Apache Sedona.
   *
   * @param nutsRegionsDf dataframe with European NUTS L1 data
   * @return
   */
  def extractRegionsGeometryDf(nutsRegionsDf: DataFrame): DataFrame = {
    // Converting WKT region shape into Sedona geometry
    val geometryRegionalsDf = nutsRegionsDf.selectExpr("NUTS_ID", "ST_GeomFromWKT(WKT) AS geometry")

    // Building SpatialRDD that allows to build an index.
    // Not critical with the current size, but given performance boost in big datasets
    val regionsSpatialRDD = Adapter.toSpatialRdd(geometryRegionalsDf, "geometry")
    regionsSpatialRDD.analyze()
    regionsSpatialRDD.spatialPartitioning(GridType.KDBTREE)
    regionsSpatialRDD.buildIndex(IndexType.QUADTREE, true)
    regionsSpatialRDD.CRSTransform("EPSG:4326", "EPSG:4326") // WGS84

    val spatialDf = Adapter.toDf(regionsSpatialRDD, nutsRegionsDf.sparkSession)
    spatialDf
  }
}
