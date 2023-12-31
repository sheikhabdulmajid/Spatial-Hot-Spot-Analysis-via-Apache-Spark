package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath)
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 0))
    spark.udf.register("CalculateY", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 1))
    spark.udf.register("CalculateZ", (pickupTime: String) => HotcellUtils.CalculateCoordinate(pickupTime, 2))
    pickupInfo = spark.sql("SELECT CalculateX(nyctaxitrips._c5), CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) FROM nyctaxitrips")
    val newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    pickupInfo.createOrReplaceTempView("pickupInfoView")

    // Extracting relevant data points within specified boundaries
    val filteredPoints = spark.sql(
      s"""
         |SELECT x, y, z, COUNT(*) AS pointCount
         |FROM pickupInfoView
         |WHERE x >= $minX AND x <= $maxX AND y >= $minY AND y <= $maxY AND z >= $minZ AND z <= $maxZ
         |GROUP BY x, y, z
         |""".stripMargin
    ).persist()

    filteredPoints.createOrReplaceTempView("filteredPointsView")

    // Calculating aggregations and statistics
    val aggregationResult = spark.sql(
      """
        |SELECT
        |  SUM(pointCount) AS totalPoints,
        |  SUM(pointCount * pointCount) AS sumOfSquares
        |FROM filteredPointsView
        |""".stripMargin
    ).persist()

    val totalPoints = aggregationResult.first().getLong(0).toDouble
    val sumOfSquares = aggregationResult.first().getLong(1).toDouble

    val mean = totalPoints / numCells
    val standardDeviation = Math.sqrt((sumOfSquares / numCells) - (mean * mean))

    // Identifying neighboring points and calculating the Z-scores
    val neighboringPoints = spark.sql(
      """
        |SELECT
        |  gp1.x AS x,
        |  gp1.y AS y,
        |  gp1.z AS z,
        |  COUNT(*) AS numOfNeighbors,
        |  SUM(gp2.pointCount) AS sigma
        |FROM filteredPointsView AS gp1
        |INNER JOIN filteredPointsView AS gp2
        |ON (ABS(gp1.x - gp2.x) <= 1 AND ABS(gp1.y - gp2.y) <= 1 AND ABS(gp1.z - gp2.z) <= 1)
        |GROUP BY gp1.x, gp1.y, gp1.z
        |""".stripMargin
    ).persist()

    neighboringPoints.createOrReplaceTempView("neighboringPointsView")

    spark.udf.register("calculateZScore", (mean: Double, stddev: Double, numOfNb: Int, sigma: Int, numCells: Int) =>
      HotcellUtils.calculateZScore(mean, stddev, numOfNb, sigma, numCells)
    )

    val resultWithZScores = spark.sql(
      s"""
         |SELECT
         |  x,
         |  y,
         |  z,
         |  calculateZScore($mean, $standardDeviation, numOfNeighbors, sigma, $numCells) AS zScore
         |FROM neighboringPointsView
         |ORDER BY zScore DESC
         |""".stripMargin
    ).persist()

    resultWithZScores.createOrReplaceTempView("resultWithZScoresView")

    // Ordering Z-scores
    val finalResult = spark.sql(
      """
        |SELECT x, y, z
        |FROM resultWithZScoresView
        |""".stripMargin
    )

    finalResult
  }
}
