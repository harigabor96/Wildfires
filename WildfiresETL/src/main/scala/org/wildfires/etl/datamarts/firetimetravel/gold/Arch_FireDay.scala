package org.wildfires.etl.datamarts.firetimetravel.gold

import org.apache.spark.sql.functions.{col, explode_outer}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wildfires.etl.GenericPipeline
import org.wildfires.etl.datamarts.firetimetravel.util.Functions._
import org.wildfires.util.DBUtils

case class Arch_FireDay(spark: SparkSession, curatedZonePath: String) extends GenericPipeline {

  val inputPath = s"$curatedZonePath/dm_firetimetravel_silver.db/fires/data"

  val outputDatabaseName = "dm_firetimetravel_gold"
  val outputTableName = "arch_fireday"
  val outputTablePath = s"$curatedZonePath/$outputDatabaseName.db/$outputTableName"
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
    DBUtils.createDatabaseIfNotExist(spark,s"$outputDatabaseName")

    transformedDf
      .writeStream
      .trigger(Trigger.AvailableNow())
      .outputMode("append")
      .format("delta")
      .option("path", s"../$outputTableDataPath")
      .option("checkpointLocation", outputTableCheckpointPath)
      .toTable(s"$outputDatabaseName.$outputTableName")
      .awaitTermination()

    DBUtils.optimizeTable(spark, outputDatabaseName, outputTableName)
    DBUtils.vacuumTable(spark, outputDatabaseName, outputTableName)
  }
}
