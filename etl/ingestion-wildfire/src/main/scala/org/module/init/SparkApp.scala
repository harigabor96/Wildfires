package org.module.init

import org.apache.spark.sql.SparkSession
import org.eztl.core.conf.IngestionConf
import org.eztl.core.init.IngestionSparkApp
import org.module.etl.zones._

object SparkApp extends IngestionSparkApp {

  override def run(spark: SparkSession, conf: IngestionConf): Unit = {
    val c = conf.asInstanceOf[Conf]

    c.pipeline() match {
    case "bronze.fires" =>
      bronze.tables.fires.
        Pipeline(spark, c).execute()
    case _ =>
      throw new Exception("Pipeline is not registered in the router!")
    }
  }

}
