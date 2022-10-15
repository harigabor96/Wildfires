package org.module.init

import org.apache.spark.sql.SparkSession
import org.eztl.core.conf.tGenericConf
import org.eztl.core.init.tGenericRouter
import org.module.etl._

object Router extends tGenericRouter {

  override def executePipeline(spark: SparkSession, conf: tGenericConf): Unit = {
    val c = conf.asInstanceOf[Conf]

    c.pipeline() match {
    case "silver.fires" =>
      silver.tables.fires.Pipeline(spark, c.curatedZonePath()).execute()
    case "gold.arch_fireday" =>
      gold.tables.arch_fireday.Pipeline(spark, c.curatedZonePath()).execute()
    case _ =>
      throw new Exception("Pipeline is not registered in the router!")
    }
  }

}
