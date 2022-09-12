package org.wildfires
import org.apache.spark.sql.SparkSession
import org.wildfires.workflow.SimpleWorkflow

object App {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    executeWorkflows(spark)
  }

  def createSparkSession(): SparkSession = {
    val warehouseLocation = "C:\\Users\\harig\\Desktop\\Wildfires-1\\WildfiresETL\\src\\main\\resources\\storage\\curated"

    val spark = SparkSession
      .builder()
      .appName("Wildfires")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def executeWorkflows(spark: SparkSession): Unit = {
    SimpleWorkflow(spark).execute()
  }
}
