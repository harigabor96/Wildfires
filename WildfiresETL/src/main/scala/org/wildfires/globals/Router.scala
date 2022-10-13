package org.wildfires.globals

import org.apache.spark.sql.SparkSession
import org.wildfires.etl.{bronze, datamarts}

object Router {

  def executePipeline(spark: SparkSession, conf: Conf): Unit = conf.pipeline() match {
    case "bronze.wildfire.fires" =>
      bronze.wildfire.Fires(spark, conf.rawZonePath(), conf.curatedZonePath(), conf.ingestPreviousDays.toOption).execute()
    case "datamarts.firetimetravel.silver.fires" =>
      datamarts.firetimetravel.silver.Fires(spark, conf.curatedZonePath()).execute()
    case "datamarts.firetimetravel.gold.fires" =>
      datamarts.firetimetravel.gold.Arch_FireDay(spark, conf.curatedZonePath()).execute()
    case _ =>
      throw new Exception("Pipeline is not registered in the router!")
  }

}
