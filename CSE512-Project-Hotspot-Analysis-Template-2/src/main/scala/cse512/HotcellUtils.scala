package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match {
      case 0 =>
        result = Math
          .floor(
            (inputString
              .split(",")(0)
              .replace("(", "")
              .toDouble / coordinateStep)
          )
          .toInt
      case 1 =>
        result = Math
          .floor(
            inputString.split(",")(1).replace(")", "").toDouble / coordinateStep
          )
          .toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result =
          HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // checks if two set of points are adjacent to each other
  // a bit different than phase 1 ST_Within
  def ST_Within(
      x1: Int,
      y1: Int,
      z1: Int,
      x2: Int,
      y2: Int,
      z2: Int
  ): Boolean = {
    val relativeX = math.abs(x1 - x2)
    val relativeY = math.abs(y1 - y2)
    val relativeZ = math.abs(z1 - z2)

    // basically if two cells are adjacent, the x, y and z coordinate will
    // not have differnce of more than 1 unit
    if (relativeX > 1 || relativeY > 1 || relativeZ > 1) {
      return false
    } else {
      return true
    }
  }

  // refer the formula to understand this function
  def Getis_Score(
      sum_wij: Double,
      sum_wij_sqr: Double,
      sum_wijxj: Double,
      sqr_sum_wij: Double,
      mean: Double,
      sd: Double,
      numCells: Double
  ): Double = {

    val numerator = sum_wijxj - mean * sum_wij
    val denominator = sd * math.sqrt(
      (numCells * sum_wij_sqr - sqr_sum_wij) / (numCells - 1)
    )
    return numerator / denominator
  }

}
