package org.wildfires

import org.apache.spark.sql.SparkSession
import org.wildfires.etl.PipelineRunner

object App {
  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    /*
     "local",
     "../storage/raw",
     "../storage/curated",
     "bronze.wildfire.fires" *
     "datamarts.firetimetravel.silver.fires"
     "datamarts.firetimetravel.gold.fires"
    */

    val spark = SparkSession
      .builder()
      .appName("Wildfires")
      .config("spark.sql.warehouse.dir", conf.curatedZonePath())
      .master(conf.master())
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    PipelineRunner.run(spark, conf)
  }
}
