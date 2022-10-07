package org.wildfires.etl.datamarts.firetimetravel.silver

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.wildfires.etl.datamarts.firetimetravel.utils.Functions._
import io.delta.tables.DeltaTable
import org.wildfires.globals.GenericPipeline

case class Fires (spark: SparkSession, curatedZonePath: String) extends GenericPipeline {

  val inputPath = s"$curatedZonePath/bronze_wildfire.db/fires/data"

  val outputDatabaseName = "dm_firetimetravel_silver"
  val outputTableName = "fires"
  val outputDataRelativePath = s"$outputDatabaseName.db/$outputTableName/data"
  val outputCheckpointRelativePath = s"$outputDatabaseName.db/$outputTableName/checkpoint"

  override def execute(): Unit = {
    load(extract())
  }

  override protected def extract(): DataFrame = {
    spark
      .readStream
      .format("delta")
      .load(inputPath)
  }

  override protected def transform(extractedDf: DataFrame): DataFrame = {
    extractedDf
      .filter(
        col("FOD_ID").isNotNull && col("FOD_ID") =!= "" &&
        col("FIRE_YEAR").isNotNull && col("FIRE_YEAR") =!= "" &&
        col("LATITUDE").isNotNull && col("LATITUDE") =!= "" &&
        col("LONGITUDE").isNotNull && col("LONGITUDE") =!= "" &&
        col("DISCOVERY_DOY").isNotNull && col("DISCOVERY_DOY") =!= ""
      )
      .withColumn("DiscoveryDate",
        getDate(col("FIRE_YEAR"), col("DISCOVERY_DOY"))
      )
      .withColumn("ContDate",
        when(
          col("CONT_DOY").isNull || col("CONT_DOY") === "",
          getDate(col("FIRE_YEAR"), col("DISCOVERY_DOY"))
        )
        .otherwise(
          getDate(col("FIRE_YEAR"), col("CONT_DOY"))
        )
      )
      .groupBy(
        col("DiscoveryDate").cast("date").as("DiscoveryDate"),
        col("ContDate").cast("date").as("ContDate"),
        col("LATITUDE").cast("double").as("LATITUDE"),
        col("LONGITUDE").cast("double").as("LONGITUDE")
      )
      .agg(
        first(col("FOD_ID")).as("FOD_ID")
      )
  }

  override protected def load(transformedDf: DataFrame): Unit = {
    transformedDf
      .writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", s"$curatedZonePath/$outputCheckpointRelativePath")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        val transformedBatch = transform(batchDF)

        if (batchId == 0) {
          transformedBatch
            .write
            .partitionBy("DiscoveryDate")
            .format("delta")
            .mode("overwrite")
            .save(s"$curatedZonePath/$outputDataRelativePath")

          spark.sql(s"CREATE DATABASE IF NOT EXISTS $outputDatabaseName")
          spark.sql(s"CREATE TABLE $outputDatabaseName.$outputTableName USING DELTA LOCATION '$outputDataRelativePath'")
        }

        if (batchId != 0) {
          import spark.implicits._

          transformedBatch.cache()

          val batchEventDates =
            transformedBatch
              .select(col("DiscoveryDate").cast("string"))
              .dropDuplicates()
              .map(_.getString(0))
              .collect()
              .mkString("','")

          DeltaTable
            .forName(s"$outputDatabaseName.$outputTableName")
            .as("deltaTable")
            .merge(
              transformedBatch.as("updates"),
              s"""
                  deltaTable.DiscoveryDate IN ('$batchEventDates')
                  AND
                  (
                    deltaTable.DiscoveryDate <=> updates.DiscoveryDate AND
                    deltaTable.ContDate <=> updates.ContDate AND
                    deltaTable.LATITUDE <=> updates.LATITUDE AND
                    deltaTable.LONGITUDE <=> updates.LONGITUDE
                  )
              """
            )
            .whenNotMatched()
            .insertAll()
            .execute()

          spark.sqlContext.clearCache()
        }
      }
      .start()
      .awaitTermination()

    DeltaTable.forName(spark,s"$outputDatabaseName.$outputTableName").optimize().executeCompaction()
    DeltaTable.forName(spark,s"$outputDatabaseName.$outputTableName").vacuum()
  }

}
