package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    val rectCoords = queryRectangle.split(",")
    val pointCoords = pointString.split(",")

    val pointX: Double = pointCoords(0).trim.toDouble
    val pointY: Double = pointCoords(1).trim.toDouble

    val rectX1: Double = math.min(rectCoords(0).trim.toDouble, rectCoords(2).trim.toDouble)
    val rectY1: Double = math.min(rectCoords(1).trim.toDouble, rectCoords(3).trim.toDouble)
    val rectX2: Double = math.max(rectCoords(0).trim.toDouble, rectCoords(2).trim.toDouble)
    val rectY2: Double = math.max(rectCoords(1).trim.toDouble, rectCoords(3).trim.toDouble)

    val isPointInsideRectangle: Boolean =
      (pointX >= rectX1) && (pointX <= rectX2) && (pointY >= rectY1) && (pointY <= rectY2)

    return isPointInsideRectangle
  }
    
}
