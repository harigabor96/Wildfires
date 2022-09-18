package org.wildfires.etl.datamarts.firetimetravel.gold

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wildfires.etl.GenericPipeline
import org.wildfires.service.DBService

case class Fact_Fire(spark: SparkSession) extends GenericPipeline {

  val inputPath = "../storage/curated/firetimetravel_silver.db/fires/data"
  val warehousePath = spark.conf.get("spark.sql.warehouse.dir")
  val outputDatabaseName ="firetimetravel_gold"
  val outputTableName = "fact_fire"
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
        col("FOD_ID"),
        col("FIRE_YEAR"),
        col("DiscoveryDate"),
        col("ContDate"),
        col("LATITUDE"),
        col("LONGITUDE"),
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
