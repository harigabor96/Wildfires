package org.wildfires.etl

import org.apache.spark.sql.SparkSession
import org.wildfires.util.AppConfig

object PipelineRunner {
  def run(spark: SparkSession, appConfig: AppConfig): Unit = appConfig.pipeline match {
    case "bronze.wildfire.fires" => bronze.wildfire.Fires(spark, appConfig.rawZonePath, appConfig.curatedZonePath).execute()
    case "datamarts.firetimetravel.silver.fires" => datamarts.firetimetravel.silver.Fires(spark, appConfig.curatedZonePath).execute()
    case "datamarts.firetimetravel.gold.fires" => datamarts.firetimetravel.gold.Arch_FireDay(spark, appConfig.curatedZonePath).execute()
  }
}
