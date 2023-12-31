# Spatial-Hot-Spot-Analysis-via-Apache-Spark

Two distinct hot spot analysis tasks were performed: Hot Zone Analysis and Hot Cell Analysis. 

The primary goal was to identify spatial patterns and statistically significant hot spots in NYC taxi trip datasets.

In HotzoneAnalysis.scala, a range join operation on a rectangle dataset and a point dataset was performed. 

The ST_Contains function in HotzoneUtils.scala determined whether a point fell within a given rectangle. 

The resulting Data Frame was then aggregated to calculate the hotness of each rectangle, providing a count of points within each zone.

For HotcellAnalysis.scala, the project leveraged spatio-temporal big data to calculate the Getis-Ord statistic of NYC Taxi Trip datasets.

The CalculateCoordinate function in HotcellUtils.scala assigned cell coordinates based on pickup points, considering the x, y, and z dimensions.

The analysis included extracting relevant data points within specified boundaries, calculating aggregations and statistics, identifying neighboring points, and ultimately computing Z-scores for each cell.

After editing the scala files for Hot zone and Hot cell Analysis, a jar file was created using the sbt assembly command, which was then tested to confirm the correctness of the Scala code.
