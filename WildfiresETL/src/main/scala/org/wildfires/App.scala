package org.wildfires
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    executeWorkflows(spark)
  }

  def createSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Wildfires")
      .config("spark.sql.warehouse.dir", "src/main/resources/storage/curated")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def executeWorkflows(spark: SparkSession): Unit = {
    //WorkflowTester(spark).runBronzePipelines()
    WorkflowTester(spark).runSilverPipelines()
  }
}
