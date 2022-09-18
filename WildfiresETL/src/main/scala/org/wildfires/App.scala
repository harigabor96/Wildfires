package org.wildfires

import org.apache.spark.sql.SparkSession
import org.wildfires.util.AppConfig

object App {
  def main(args: Array[String]): Unit = {

    val appConfig = AppConfig(
      "../storage/raw",
      "../storage/curated"
      //pipelineToRun Should come from args[]
    )

    val spark = SparkSession
      .builder()
      .appName("Wildfires")
      .config("spark.sql.warehouse.dir", appConfig.warehousePath)
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    run(spark, appConfig)
  }

  def run(spark: SparkSession, appConfig: AppConfig): Unit = {
    import org.wildfires.etl._

    //bronze.wildfire.Fires(spark, appConfig.rawZonePath).execute()
    //datamarts.firetimetravel.silver.Fires(spark).execute()
    datamarts.firetimetravel.gold.Arch_Fire(spark).execute()

    //val bronzeDf = spark.read.format("delta").load("../storage/curated/bronze_wildfire.db/fires/data")
    //val silver = spark.read.format("delta").load("../storage/curated/dm_firetimetravel_silver.db/fires/data")
    //val gold = spark.read.format("delta").load("../storage/curated/dm_firetimetravel_gold.db/arch_fire/data")
  }
}
