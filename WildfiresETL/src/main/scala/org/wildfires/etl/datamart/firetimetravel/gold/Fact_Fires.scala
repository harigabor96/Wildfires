package org.wildfires.etl.datamart.firetimetravel.gold

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wildfires.etl.GenericPipeline
import org.wildfires.service.{DBService, FileService}

case class Fact_Fires(spark: SparkSession) extends GenericPipeline {

  //This will eventually moved to a config class
  val inputPath = "src/main/resources/storage/curated/firetimetravel_silver.db/fires/data"

  val timeoutMs = 600000

  val warehousePath = spark.conf.get("spark.sql.warehouse.dir")
  val outputDatabaseName ="firetimetravel_gold"
  val outputTableName = "fact_fires"
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
  }

  override def load(transformedData: DataFrame): Unit = {

    transformedData
      .writeStream
      .option("checkpointLocation", outputTableCheckpointPath)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

          transform(batchDF)
            .write
            .format("delta")
            .mode("overwrite")
            .save(s"$outputTableDataPath")

          if (batchId == 0) {
            DBService.createDatabaseIfNotExist(spark, outputDatabaseName)
            DBService.createDeltaTableFromPath(spark, outputDatabaseName, outputTableName, outputTableDataPath)
          }
        }
      }
      .start()
      .awaitTermination(timeoutMs)

    DBService.optimizeTable(spark, outputDatabaseName, outputTableName)
    DBService.vacuumTable(spark, outputDatabaseName, outputTableName)
}