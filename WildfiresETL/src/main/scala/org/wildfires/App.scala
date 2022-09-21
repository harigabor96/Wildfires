package org.wildfires

import org.apache.spark.sql.SparkSession
import org.wildfires.etl.PipelineRunner
import org.wildfires.util.AppConfig

object App {
  def main(args: Array[String]): Unit = {

    val appConfig = AppConfig(
      "local",
      "../storage/raw",
      "../storage/curated",
      "bronze.wildfire.fires"
      //"datamarts.firetimetravel.silver.fires"
      //"datamarts.firetimetravel.gold.fires"
    )

    val spark = SparkSession
      .builder()
      .appName("Wildfires")
      .config("spark.sql.warehouse.dir", appConfig.curatedZonePath)
      .master(appConfig.master)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    PipelineRunner.run(spark, appConfig)
  }
}
