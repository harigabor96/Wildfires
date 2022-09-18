package org.wildfires.etl.datamarts.firetimetravel.gold

import org.apache.spark.sql.functions.{col, explode_outer}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wildfires.etl.{GenericPipeline, PipelineConfig}
import org.wildfires.etl.datamarts.firetimetravel.util.Functions._
import org.wildfires.service.DBService

case class Fact_Fire(spark: SparkSession, config: PipelineConfig) extends GenericPipeline {

  val inputPath = config.inputPath
  val warehousePath = spark.conf.get("spark.sql.warehouse.dir")
  val outputDatabaseName = config.outputDatabase
  val outputTableName = config.outputTable
  val outputTablePath = s"$warehousePath/$outputDatabaseName.db/$outputTableName"
  val outputTableDataPath = s"$outputTablePath/data"
  val outputTableCheckpointPath = s"$outputTablePath/checkpoint"

  override def execute(): Unit = {
    load(transform(extract()))
  }

  override def extract(): DataFrame = {
    spark
      .readStream
      .format("delta")
      .load(inputPath)
  }

  override def transform(extractedDf: DataFrame): DataFrame = {
    extractedDf
      .select(
        col("FOD_ID").as("FireID"),
        explode_outer(
          daysFromInterval(col("DiscoveryDate"), col("ContDate"))
        ).as("Date"),
        col("LATITUDE").as("Latitude"),
        col("LONGITUDE").as("Longitude"),
      )
  }

  override def load(transformedDf: DataFrame): Unit = {
    DBService.createDatabaseIfNotExist(spark,s"$outputDatabaseName")

    transformedDf
      .writeStream
      .trigger(Trigger.AvailableNow())
      .outputMode("append")
      .format("delta")
      .option("path", outputTableDataPath)
      .option("checkpointLocation", outputTableCheckpointPath)
      .toTable(s"$outputDatabaseName.$outputTableName")
      .awaitTermination()

    DBService.optimizeTable(spark, outputDatabaseName, outputTableName)
    DBService.vacuumTable(spark, outputDatabaseName, outputTableName)
  }
}
