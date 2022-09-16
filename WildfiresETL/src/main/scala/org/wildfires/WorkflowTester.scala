package org.wildfires

import org.apache.spark.sql.SparkSession

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
    import org.wildfires.etl.datamart._

    firetimetravel.gold.Fact_Fires(spark).execute()
  }
}
