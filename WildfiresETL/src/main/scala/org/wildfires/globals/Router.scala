package org.wildfires.globals

import org.apache.spark.sql.SparkSession
import org.eztl.core.conf.tGenericConf
import org.eztl.core.init.tGenericRouter
import org.wildfires.etl.{bronze, datamarts}

object Router extends tGenericRouter {

  override def executePipeline(spark: SparkSession, conf: tGenericConf): Unit = {
    val c = conf.asInstanceOf[Conf]

    c.pipeline() match {
    case "bronze.wildfire.fires" =>
      bronze.wildfire.Fires(spark, c.rawZonePath(), c.curatedZonePath(), c.ingestPreviousDays.toOption).execute()
    case "datamarts.firetimetravel.silver.fires" =>
      datamarts.firetimetravel.silver.Fires(spark, c.curatedZonePath()).execute()
    case "datamarts.firetimetravel.gold.fires" =>
      datamarts.firetimetravel.gold.Arch_FireDay(spark, c.curatedZonePath()).execute()
    case _ =>
      throw new Exception("Pipeline is not registered in the router!")
    }
  }

}
