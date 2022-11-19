package org.module.init

import org.apache.spark.sql.SparkSession
import org.eztl.core.conf.DatamartConf
import org.eztl.core.init.DatamartSparkApp
import org.module.etl.zones._

object SparkApp extends DatamartSparkApp {

  override def run(spark: SparkSession, conf: DatamartConf): Unit = {
    val c = conf.asInstanceOf[Conf]

    c.pipeline() match {
    case "silver.fires" =>
      silver.tables.fires.
        Pipeline(spark, c).execute()
    case "gold.arch_fireday" =>
      gold.tables.arch_fireday
        .Pipeline(spark, c).execute()
    case _ =>
      throw new Exception("Pipeline is not registered in the router!")
    }
  }

}
