package org.wildfires

import org.apache.spark.sql.SparkSession
import org.wildfires.etl.bronze.wildfire.Fires
import org.wildfires.etl.datamart.firetimetravel.silver

case class WorkflowTester(spark: SparkSession) {

  def runBronzePipelines(): Unit = {
    import org.wildfires.etl.bronze._

    wildfire.Fires(spark).execute()
  }

  def runSilverPipelines(): Unit = {
    import org.wildfires.etl.datamart._

    firetimetravel.silver.Fires(spark).execute()
  }

  def runGoldPipelines(): Unit = {

  }
}
