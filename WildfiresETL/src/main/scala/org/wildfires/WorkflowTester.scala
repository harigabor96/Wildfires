package org.wildfires

import org.apache.spark.sql.SparkSession

case class WorkflowTester(spark: SparkSession) {

  def runBronzePipelines(): Unit = {
    import org.wildfires.etl.bronzemodules._

    wildfire.Fires(spark).execute()
  }

  def runSilverPipelines(): Unit = {
    import org.wildfires.etl.silvergoldmodules.firetimetravel._

    //silver.Fires(spark).execute()
  }

  def runGoldPipelines(): Unit = {

  }
}
