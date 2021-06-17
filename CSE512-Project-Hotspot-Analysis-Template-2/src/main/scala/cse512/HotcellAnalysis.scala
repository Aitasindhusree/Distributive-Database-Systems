package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
// import spark.implicits._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "false")
      .load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    // pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register(
      "CalculateX",
      (pickupPoint: String) =>
        ((
          HotcellUtils.CalculateCoordinate(
            pickupPoint,
            0
          )
        ))
    )
    spark.udf.register(
      "CalculateY",
      (pickupPoint: String) =>
        ((
          HotcellUtils.CalculateCoordinate(
            pickupPoint,
            1
          )
        ))
    )
    spark.udf.register(
      "CalculateZ",
      (pickupTime: String) =>
        ((
          HotcellUtils.CalculateCoordinate(
            pickupTime,
            2
          )
        ))
    )

    // register function which will tell if the two points are adjacent to each other
    // helps in calculating w_{i,j}
    spark.udf.register(
      "ST_Within",
      (x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int) =>
        ((
          HotcellUtils.ST_Within(
            x1,
            y1,
            z1,
            x2,
            y2,
            z2
          )
        ))
    )

    spark.udf.register(
      "Getis_Score",
      (
          sum_wij: Double,
          sum_wij_sqr: Double,
          sum_wijxj: Double,
          sqr_sum_wij: Double,
          mean: Double,
          sd: Double,
          numCells: Double
      ) =>
        ((
          HotcellUtils.Getis_Score(
            sum_wij,
            sum_wij_sqr,
            sum_wijxj,
            sqr_sum_wij,
            mean,
            sd,
            numCells
          )
        ))
    )

    pickupInfo = spark.sql(
      "select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips"
    )
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.createOrReplaceTempView("pickupCoordinates")
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    Console.println(s"Range for X -> ${minX} to ${maxX}")
    Console.println(s"Range for Y -> ${minY} to ${maxY}")
    Console.println(s"Number cells -> ${numCells}")

    // points that lie inside the required boundary
    // denotes variable 'x' in the formula
    val countPointsInCell = spark
      .sql(s"select * from pickupCoordinates")
      .filter(
        col("x") >= minX && col("x") <= maxX
          && col("y") >= minY && col("y") <= maxY
          && col("z") >= minZ && col("z") <= maxZ
      )
      // to group all points in the same cell
      .groupBy("x", "y", "z")
      // count number of points in the same cell
      // calculates the variable 'x' (num of pts in same cube)
      .count()
    countPointsInCell.createOrReplaceTempView("countPointsInCell")
    countPointsInCell.show()

    // df to find the cells that are adjacent to each other
    // calculates wij, basically, if point i and point j are adjacent, wij = 1 else 0
    val adjacentCells = spark
      .sql(
        "SELECT i.x as xi, i.y as yi, i.z as zi, j.x as xj, j.y as yj, j.z as zy, j.count as numcells_x, CASE WHEN ST_WITHIN(i.x, i.y, i.z, j.x, j.y, j.z) THEN 1 ELSE 0 END as wij from countPointsInCell i, countPointsInCell j"
      )
    adjacentCells.createOrReplaceTempView("adjacentCells")
    adjacentCells.show()

    // find the mean of 'x' (mean num of pts in same cube)
    val meanX = countPointsInCell
      .agg(sum("count"))
      .first
      .getLong(0) / numCells
    Console.println(s"Mean is ${meanX}")

    // use the mean to calculate standard deviation of 'x'
    val countSquared = countPointsInCell
      .withColumn("count_squared", col("count") * col("count"))
    countSquared.createOrReplaceTempView("countSquared")

    val standardDeviationX = math.sqrt(
      (countSquared
        .agg(sum("count_squared"))
        .first
        .getLong(0) / numCells) - math.pow(meanX, 2)
    )
    Console.println(s"Standard deviation -> ${standardDeviationX}")

    // calculates the necessary parts needed to calculate gscore
    val for_getis_score = adjacentCells
      // (wij)^2
      .withColumn("wij_sqr", col("wij") * col("wij"))
      // wij * xj
      .withColumn("wijxj", col("wij") * col("numcells_x"))
      // to calculate sums
      .groupBy("xi", "yi", "zi")
      .agg(
        // sum of wij
        sum("wij") as "sum_wij",
        // sum of (wij^2)
        sum("wij_sqr") as "sum_wij_sqr",
        // sum of (wij * xj)
        sum("wijxj") as "sum_wijxj"
      )
      // square of (sum of wij)
      .withColumn("sqr_sum_wij", col("sum_wij") * col("sum_wij"))
    for_getis_score.createOrReplaceTempView("for_getis_score")
    for_getis_score.show()

    // calculates the gscore by passing relevant columns to the registered UDF (user defined function)
    // check the formula of g score to better understand what this function is doing (defined in hotcellutils)
    val getis_score = spark
      .sql(
        s"select f.xi, f.yi, f.zi, Getis_score(f.sum_wij,f.sum_wij_sqr,f.sum_wijxj,f.sqr_sum_wij,${meanX},${standardDeviationX},${numCells}) as g from for_getis_score f "
      )
      // sort according to descending g score values and
      // select 50 vals and only x,y,z coordinates
      .sort(desc("g"))
      .limit(50)
      .select("xi", "yi", "zi")
    getis_score.createOrReplaceTempView("getis_score")

    // return the gscore dataframe
    return getis_score
  }
}
