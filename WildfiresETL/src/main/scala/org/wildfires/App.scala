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
    import org.wildfires.etl._
/*
    bronze.wildfire.Fires(
        spark,
        PipelineConfig(
          "../storage/raw/FPA_FOD_20170508/{*}/in",
          "bronze_wildfire",
          "fires"
        )
      ).execute()

    datamarts.firetimetravel.silver.Fires(
      spark,
      PipelineConfig(
        "../storage/curated/bronze_wildfire.db/fires/data",
        "dm_firetimetravel_silver",
        "fires"
      )
    ).execute()

    datamarts.firetimetravel.gold.Fact_Fire(
      spark,
      PipelineConfig(
        "../storage/curated/dm_firetimetravel_silver.db/fires/data",
        "dm_firetimetravel_gold",
        "fact_fire"
      )
      ).execute()
    */

    //val bronzeDf = spark.read.format("delta").load("../storage/curated/bronze_wildfire.db/fires/data")
    //val silver = spark.read.format("delta").load("../storage/curated/dm_firetimetravel_silver.db/fires/data")
    //val gold = spark.read.format("delta").load("../storage/curated/dm_firetimetravel_gold.db/fact_fire/data")
  }
}
