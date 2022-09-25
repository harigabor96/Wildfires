package org.wildfires.etl

import org.apache.spark.sql.SparkSession
import org.wildfires.Conf

object PipelineRunner {

  def run(spark: SparkSession, conf: Conf): Unit = conf.pipeline() match {
    case "bronze.wildfire.fires" => bronze.wildfire.Fires(spark, conf.rawZonePath(), conf.curatedZonePath()).execute()
    case "datamarts.firetimetravel.silver.fires" => datamarts.firetimetravel.silver.Fires(spark, conf.curatedZonePath()).execute()
    case "datamarts.firetimetravel.gold.fires" => datamarts.firetimetravel.gold.Arch_FireDay(spark, conf.curatedZonePath()).execute()
  }

}
