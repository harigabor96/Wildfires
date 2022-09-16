package org.wildfires.etl.datamart.firetimetravel.silver

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{first, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wildfires.etl.GenericPipeline
import org.wildfires.service._
import org.wildfires.etl.datamart.firetimetravel.util.Functions._

case class Fires (spark: SparkSession) extends GenericPipeline {

  val inputPath = "../storage/curated/bronze_wildfire.db/fires/data"

  val timeoutMs = 600000

  val warehousePath = spark.conf.get("spark.sql.warehouse.dir")
  val outputDatabaseName ="firetimetravel_silver"
  val outputTableName = "fires"
  val outputTablePath = s"$warehousePath/$outputDatabaseName.db/$outputTableName"
  val outputTableDataPath = s"$outputTablePath/data"
  val outputTableCheckpointPath = s"$outputTablePath/checkpoint"

  override def execute(): Unit = {
    load(extract())
  }

  override def extract(): DataFrame = {
    spark
      .readStream
      .format("delta")
      .load(inputPath)
  }

  override def transform(extractedDf: DataFrame): DataFrame = {

    extractedDf
      .filter(
        col("FOD_ID").isNotNull && col("FOD_ID") =!= "" &&
        col("FIRE_YEAR").isNotNull && col("FIRE_YEAR") =!= "" &&
        col("DISCOVERY_DOY").isNotNull && col("DISCOVERY_DOY") =!= "" &&
        col("CONT_DOY").isNotNull && col("CONT_DOY") =!= "" &&
        col("LATITUDE").isNotNull && col("LATITUDE") =!= "" &&
        col("LONGITUDE").isNotNull && col("LONGITUDE") =!= ""
      )
      .groupBy(
        col("FOD_ID")
      )
      .agg(
        first("ExtractionDate").as("ExtractionDate"),
        first("FIRE_YEAR").as("FIRE_YEAR"),
        first("DISCOVERY_DOY").as("DISCOVERY_DOY"),
        first("CONT_DOY").as("CONT_DOY"),
        first("LATITUDE").as("LATITUDE"),
        first("LONGITUDE").as("LONGITUDE")
      )
      .select(
        col("ExtractionDate"),
        col("FOD_ID"),
        getDate(col("FIRE_YEAR"), col("DISCOVERY_DOY")).as("DiscoveryDate"),
        getDate(col("FIRE_YEAR"), col("CONT_DOY")).as("ContDate"),
        col("LATITUDE").cast(DoubleType),
        col("LONGITUDE").cast(DoubleType)
      )
  }

  override def load(transformedDf: DataFrame): Unit = {

    transformedDf
      .writeStream
      .option("checkpointLocation", outputTableCheckpointPath)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        val transformedBatch = transform(batchDF)

        if (batchId == 0) {
          transformedBatch
            .write
            .partitionBy("DiscoveryDate")
            .format("delta")
            .mode("overwrite")
            .save(s"$outputTableDataPath")

          DBService.createDatabaseIfNotExist(spark, outputDatabaseName)
          DBService.createDeltaTableFromPath(spark, outputDatabaseName, outputTableName, outputTableDataPath)
        }

        import spark.implicits._

        val batchEventDates =
          transformedBatch
            .select(col("DiscoveryDate"))
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
                  deltaTable.FOD_ID <=> updates.FOD_ID
                )
            """
          )
          .whenNotMatched()
          .insertAll()
          .execute()
      }
      .start()
      .awaitTermination(timeoutMs)

    DBService.optimizeTable(spark, outputDatabaseName, outputTableName)
    DBService.vacuumTable(spark, outputDatabaseName, outputTableName)
  }
}
