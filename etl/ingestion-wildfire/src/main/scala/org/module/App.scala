package org.module

import org.apache.spark.sql.SparkSession
import org.module.init.{Conf, SparkApp}

object App {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    /*
    val conf = new Conf(Array(
      "-m", "local",
      "-r", "../../storage/raw/",
      "-c", "../../storage/curated/",
      "-p", "bronze.fires" //, "-i", ""
    ))
    */

    val spark = SparkSession
      .builder()
      .appName("ingestion-wildfire")
      .config("spark.sql.warehouse.dir", conf.curatedZonePath())
      .master(conf.master())
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    SparkApp.run(spark, conf)
  }

}
