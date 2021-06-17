package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val ptArray = pointString.split(",")
    val ptX = ptArray(0).trim().toDouble
    val ptY = ptArray(1).trim().toDouble

    val rectArray = queryRectangle.split(",")
    val rect1X = rectArray(0).trim().toDouble
    val rect1Y = rectArray(1).trim().toDouble
    val rect2X = rectArray(2).trim().toDouble
    val rect2Y = rectArray(3).trim().toDouble

    // define the rectangle boundaries
    // equality cannot exist, as it wouldn't be a rectangle otherwise
    val upperXBoundary = if (rect1X > rect2X) rect1X else rect2X
    val lowerXBoundary = if (rect1X < rect2X) rect1X else rect2X

    val upperYBoundary = if (rect1Y > rect2Y) rect1Y else rect2Y
    val lowerYBoundary = if (rect1Y < rect2Y) rect1Y else rect2Y

    // if point is inside the boundary then true, else false
    if (
      ptX >= lowerXBoundary && ptX <= upperXBoundary && ptY >= lowerYBoundary && ptY <= upperYBoundary
    ) {
      return true
    }
    return false
  }
}
