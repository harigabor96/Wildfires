package org.wildfires.etl.datamarts.firetimetravel.silver

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{first, _}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wildfires.etl.{GenericPipeline, PipelineConfig}
import org.wildfires.service._
import org.wildfires.etl.datamarts.firetimetravel.util.Functions._

case class Fires (spark: SparkSession, config: PipelineConfig) extends GenericPipeline {

  val inputPath = config.inputPath
  val warehousePath = spark.conf.get("spark.sql.warehouse.dir")
  val outputDatabaseName = config.outputDatabase
  val outputTableName = config.outputTable
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
        col("FIRE_YEAR"),
        col("DiscoveryDate"),
        col("ContDate"),
        col("LATITUDE"),
        col("LONGITUDE")
      )
      .agg(
        first(col("FOD_ID")).as("FOD_ID")
      )
  }

  override def load(transformedDf: DataFrame): Unit = {

    transformedDf
      .writeStream
      .trigger(Trigger.AvailableNow())
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
                  deltaTable.FIRE_YEAR <=> updates.FIRE_YEAR AND
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
      }
      .start()
      .awaitTermination()

    DBService.optimizeTable(spark, outputDatabaseName, outputTableName)
    DBService.vacuumTable(spark, outputDatabaseName, outputTableName)
  }
}
