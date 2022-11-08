package org.module.etl.zones.gold.tables.arch_fireday

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.eztl.core.etl.tGenericPipeline
import Functions._

case class Pipeline(spark: SparkSession, curatedZonePath: String) extends tGenericPipeline {

  val inputPath = s"$curatedZonePath/dm_firetimetravel_silver.db/fires/data"

  val outputDatabaseName = "dm_firetimetravel_gold"
  val outputTableName = "arch_fireday"
  val outputDataRelativePath = s"$outputDatabaseName.db/$outputTableName/data"
  val outputCheckpointRelativePath = s"$outputDatabaseName.db/$outputTableName/checkpoint"

  override def execute(): Unit = {
    load(transform(extract()))
  }

  override protected def extract(): DataFrame = {
    spark
      .readStream
      .format("delta")
      .load(inputPath)
  }

  override protected def transform(extractedDf: DataFrame): DataFrame = {
    extractedDf
      .withColumn("Date",
        explode_outer(
          days_from_interval(col("DiscoveryDate"), col("ContDate"))
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

  override protected def load(transformedDf: DataFrame): Unit = {
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
