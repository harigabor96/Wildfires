package org.wildfires
import org.apache.spark.sql.SparkSession
import org.wildfires.workflow.SimpleWorkflow

object App {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    executeWorkflows(spark)
  }

  def createSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Wildfires")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def executeWorkflows(spark: SparkSession): Unit = {
    SimpleWorkflow(spark).execute()
  }
}
