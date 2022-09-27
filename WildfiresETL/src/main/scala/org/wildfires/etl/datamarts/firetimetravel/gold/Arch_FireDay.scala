package org.wildfires.etl.datamarts.firetimetravel.gold

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.wildfires.etl.datamarts.firetimetravel.utils.Functions._
import io.delta.tables.DeltaTable
import org.wildfires.globals.GenericPipeline

case class Arch_FireDay(spark: SparkSession, curatedZonePath: String) extends GenericPipeline {

  val inputPath = s"$curatedZonePath/dm_firetimetravel_silver.db/fires/data"

  val outputDatabaseName = "dm_firetimetravel_gold"
  val outputTableName = "arch_fireday"
  val outputDataRelativePath = s"$outputDatabaseName.db/$outputTableName/data"
  val outputCheckpointRelativePath = s"$outputDatabaseName.db/$outputTableName/checkpoint"

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
      .withColumn("Date",
        explode_outer(
          daysFromInterval(col("DiscoveryDate"), col("ContDate"))
        )
      )
      .select(
        col("FOD_ID").as("FireID"),
        col("DiscoveryDate"),
        col("ContDate"),
        col("Date").cast("date").as("Date"),
        col("LATITUDE").as("Latitude"),
        col("LONGITUDE").as("Longitude"),
      )
  }

  override def load(transformedDf: DataFrame): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $outputDatabaseName")

    transformedDf
      .writeStream
      .trigger(Trigger.AvailableNow())
      .outputMode("append")
      .format("delta")
      .option("path", s"$outputDataRelativePath")
      .option("checkpointLocation", s"$curatedZonePath/$outputCheckpointRelativePath")
      .toTable(s"$outputDatabaseName.$outputTableName")
      .awaitTermination()

    DeltaTable.forName(spark,s"$outputDatabaseName.$outputTableName").optimize().executeCompaction()
    DeltaTable.forName(spark,s"$outputDatabaseName.$outputTableName").vacuum()
  }

}
