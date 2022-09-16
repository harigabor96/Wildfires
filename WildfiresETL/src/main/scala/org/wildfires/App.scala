package org.wildfires

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    executeWIP(spark)
  }

  def createSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Wildfires")
      .config("spark.sql.warehouse.dir", "../storage/curated")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def executeWIP(spark: SparkSession): Unit = {
    import org.apache.spark.sql.functions.col
    import org.wildfires.etl.bronze._
    import org.wildfires.etl.datamarts._

    /*
    spark
      .read
      .format("delta")
      .load("../storage/curated/bronze_wildfire.db/fires/data")
      .show()
      */

    /*
    val silver =
      spark
        .read
        .format("delta")
        .load("../storage/curated/firetimetravel_silver.db/fires/data")
    */

    //wildfire.Fires(spark).execute()
    //firetimetravel.silver.Fires(spark).execute()
    firetimetravel.gold.Fact_Fire(spark).execute()
  }
}
