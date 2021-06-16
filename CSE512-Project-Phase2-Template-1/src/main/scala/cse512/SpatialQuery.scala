package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    if (queryRectangle == null || queryRectangle.isEmpty() || pointString == null || pointString.isEmpty()){
      return false
    }

    val ptArray = pointString.split(",")
    val ptX = ptArray(0).trim().toDouble
    val ptY = ptArray(1).trim().toDouble

    val rectArray = queryRectangle.split(",")
    val rect1X = rectArray(0).trim().toDouble
    val rect1Y = rectArray(1).trim().toDouble
    val rect2X = rectArray(2).trim().toDouble
    val rect2Y = rectArray(3).trim().toDouble

    return (
      (rect1X - ptX) * (ptX - rect2X ) >= 0 
      && (rect1Y - ptY) * (ptY - rect2Y) >= 0
    )
  }

  def ST_Within(
      pointString1: String,
      pointString2: String,
      distance: Double
  ): Boolean = {

    if (pointString1 == null || pointString1.isEmpty() || pointString2 == null || pointString2.isEmpty() || distance <= 0.00){
      return false
    }

    val ptArray1 = pointString1.split(",")
    val pt1X = ptArray1(0).trim().toDouble
    val pt1Y = ptArray1(1).trim().toDouble

    val ptArray2 = pointString2.split(",")
    val pt2X = ptArray2(0).trim().toDouble
    val pt2Y = ptArray2(1).trim().toDouble

    val relativeX = pt1X - pt2X
    val relativeY = pt1Y - pt2Y

    val distancePt1Pt2 = math.sqrt(
      math.pow(relativeX, 2) + math.pow(relativeY, 2)
    )

    return distancePt1Pt2 <= distance
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Contains",
      (queryRectangle: String, pointString: String) =>
        ((ST_Contains(queryRectangle, pointString)))
    )

    val resultDf = spark.sql(
      "select * from point where ST_Contains('" + arg2 + "',point._c0)"
    )
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(
      spark: SparkSession,
      arg1: String,
      arg2: String
  ): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Contains",
      (queryRectangle: String, pointString: String) => ((ST_Contains(queryRectangle, pointString)))
    )

    val resultDf = spark.sql(
      "select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)"
    )
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(
      spark: SparkSession,
      arg1: String,
      arg2: String,
      arg3: String
  ): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Within",
      (pointString1: String, pointString2: String, distance: Double) =>
        ((ST_Within(pointString1, pointString2, distance)))
    )

    val resultDf = spark.sql(
      "select * from point where ST_Within(point._c0,'" + arg2 + "'," + arg3 + ")"
    )
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(
      spark: SparkSession,
      arg1: String,
      arg2: String,
      arg3: String
  ): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Within",
      (pointString1: String, pointString2: String, distance: Double) =>
        ((ST_Within(pointString1, pointString2, distance)))
    )
    val resultDf = spark.sql(
      "select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, " + arg3 + ")"
    )
    resultDf.show()

    return resultDf.count()
  }
}
