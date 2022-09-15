package org.wildfires.etl.datamart.firetimetravel.silver

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{first, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wildfires.etl.GenericPipeline
import org.wildfires.service._

case class Fires (spark: SparkSession) extends GenericPipeline {

  val inputPath = "src/main/resources/storage/curated/bronze_wildfire.db/fires/data"

  val warehousePath = spark.conf.get("spark.sql.warehouse.dir")
  val outputDatabaseName ="firetimetravel_silver"
  val outputTableName = "fires"
  val outputTablePath =
    FileService.removePathPrefix(s"$warehousePath/$outputDatabaseName.db/$outputTableName", "file:/" )
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

  override def transform(extractedData: Any): DataFrame = {
    val extractedDf = extractedData.asInstanceOf[DataFrame]

    extractedDf
      .filter(
        col("FOD_ID").isNotNull && col("FOD_ID") =!= "" &&
        col("DISCOVERY_DATE").isNotNull && col("DISCOVERY_DATE") =!= "" &&
        col("CONT_DATE").isNotNull && col("CONT_DATE") =!= "" &&
        col("LATITUDE").isNotNull && col("LATITUDE") =!= "" &&
        col("LONGITUDE").isNotNull && col("LONGITUDE") =!= ""
      )
      .groupBy(
        col("FOD_ID")
      )
      .agg(
        first("ExtractionDate").as("ExtractionDate"),
        first("DISCOVERY_DATE").as("DISCOVERY_DATE"),
        first("CONT_DATE").as("CONT_DATE"),
        first("LATITUDE").as("LATITUDE"),
        first("LONGITUDE").as("LONGITUDE")
      )
      .select(
        col("ExtractionDate"),
        col("FOD_ID"),
        col("DISCOVERY_DATE").as("DISCOVERY_DATE"),
        col("CONT_DATE").as("CONT_DATE"),
        col("LATITUDE").cast(DoubleType),
        col("LONGITUDE").cast(DoubleType)
      )
  }

  override def load(transformedData: DataFrame): Unit = {

    transformedData
      .writeStream
      .option("checkpointLocation", outputTableCheckpointPath)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        val transformedBatch = transform(batchDF)

        if (batchId == 0) {
          transformedBatch
            .write
            //.partitionBy("ExtractionDate", "DISCOVERY_DATE")
            .format("delta")
            .mode("overwrite")
            .save(s"$outputTableDataPath")

          DBService.createDatabaseIfNotExist(spark, outputDatabaseName)
          DBService.createDeltaTableFromPath(spark, outputDatabaseName, outputTableName, outputTableDataPath)

          spark.sql(s"""
              SELECT *
              FROM $outputDatabaseName.$outputTableName
          """).show()
        }

        import spark.implicits._

        val batchEventDates =
          transformedBatch
            .select(col("DISCOVERY_DATE"))
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
                deltaTable.DISCOVERY_DATE IN ('$batchEventDates')
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
      .awaitTermination(60000)

    DBService.optimizeTable(spark, outputDatabaseName, outputTableName)
    DBService.vacuumTable(spark, outputDatabaseName, outputTableName)
  }
}
