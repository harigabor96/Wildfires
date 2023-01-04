package org.module.etl.zones.silver.tables.fires

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.eztl.core.etl.GenericPipeline
import Functions._
import org.apache.spark.sql.expressions.Window
import org.module.init.Conf

case class Pipeline(spark: SparkSession, conf: Conf) extends GenericPipeline {

  val inputPath = s"${conf.curatedZonePath()}/bronze_wildfire.db/fires/data"

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
    val cleansedDf =
      extractedDf
        .filter(
          col("FOD_ID").isNotNull && col("FOD_ID") =!= "" &&
          col("FIRE_YEAR").isNotNull && col("FIRE_YEAR") =!= "" &&
          col("LATITUDE").isNotNull && col("LATITUDE") =!= "" &&
          col("LONGITUDE").isNotNull && col("LONGITUDE") =!= "" &&
          col("DISCOVERY_DOY").isNotNull && col("DISCOVERY_DOY") =!= ""
        )
        .withColumn("DiscoveryDate",
          get_date(col("FIRE_YEAR"), col("DISCOVERY_DOY"))
        )
        .withColumn("ContDate",
          when(
            col("CONT_DOY").isNull || col("CONT_DOY") === "",
            get_date(col("FIRE_YEAR"), col("DISCOVERY_DOY"))
          )
          .otherwise(
            get_date(col("FIRE_YEAR"), col("CONT_DOY"))
          )
        )
        .select(
          col("DiscoveryDate").cast("date").as("DiscoveryDate"),
          col("ContDate").cast("date").as("ContDate"),
          col("LATITUDE").cast("double").as("LATITUDE"),
          col("LONGITUDE").cast("double").as("LONGITUDE"),
          col("FOD_ID")
        )

    cleansedDf
      .withColumn("rank",
        row_number()
        .over(
          Window.partitionBy(
            "DiscoveryDate",
            "ContDate",
            "LATITUDE",
            "LONGITUDE"
          ).orderBy("DiscoveryDate")
        )
      )
      .filter(col("rank") === 1)
      .drop(col("rank"))
  }

  override protected def load(transformedDf: DataFrame): Unit = {
    transformedDf
      .writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", s"${conf.curatedZonePath()}/$outputCheckpointRelativePath")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        val transformedBatch = transform(batchDF)

        if (batchId == 0) {
          transformedBatch
            .write
            .partitionBy("DiscoveryDate")
            .format("delta")
            .save(s"${conf.curatedZonePath()}/$outputDataRelativePath")

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

    val deltaTable = DeltaTable.forName(spark, s"$outputDatabaseName.$outputTableName")
    deltaTable.optimize().executeCompaction()
    deltaTable.vacuum()
  }

}
