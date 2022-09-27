package org.wildfires

import org.apache.spark.sql.SparkSession
import org.wildfires.globals.{Conf, PipelineRooter}

object App {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    /*
    val conf = new Conf(Array(
      "-m", "local",
      "-r", "../storage/raw/",
      "-c", "../storage/curated/",
      //"-p", "bronze.wildfire.fires"
      //"-p", "datamarts.firetimetravel.silver.fires"
      //"-p", "datamarts.firetimetravel.gold.fires"
    ))
    */

    val spark = SparkSession
      .builder()
      .appName("Wildfires")
      .config("spark.sql.warehouse.dir", conf.curatedZonePath())
      .master(conf.master())
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    PipelineRooter.execute(spark, conf)
  }

}
