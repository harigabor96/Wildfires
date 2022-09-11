package org.wildfires.workflow

import org.apache.spark.sql.SparkSession

case class SimpleWorkflow(spark: SparkSession) extends GenericWorkflow {

  override def runBronzePipelines(): Unit = {
    import org.wildfires.pipeline.bronze._

    wildfire.Wildfire(spark).execute()
  }

  override def runSilverPipelines(): Unit = {

  }

  override def runGoldPipelines(): Unit = {

  }
}
