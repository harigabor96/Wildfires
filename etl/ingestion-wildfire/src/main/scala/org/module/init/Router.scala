package org.module.init

import org.apache.spark.sql.SparkSession
import org.eztl.core.conf.tGenericConf
import org.eztl.core.init.tGenericRouter
import org.module.etl._
import org.module.etl.zones.bronze.tables.fires.Pipeline

object Router extends tGenericRouter {

  override def executePipeline(spark: SparkSession, conf: tGenericConf): Unit = {
    val c = conf.asInstanceOf[Conf]

    c.pipeline() match {
    case "bronze.fires" =>
      Pipeline(spark, c.rawZonePath(), c.curatedZonePath(), c.ingestPreviousDays.toOption).execute()
    case _ =>
      throw new Exception("Pipeline is not registered in the router!")
    }
  }

}
