package org.module.etl.bronzetables.fires

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.eztl.core.etl.tGenericPipeline
import org.eztl.ingestion.FilePathBuilder
import org.module.etl.bronzetables.fires.Functions._

case class Pipeline(spark: SparkSession, rawZonePath: String, curatedZonePath: String, prevDaysToIngest: Option[Int]) extends tGenericPipeline {

  val inputPath = s"$rawZonePath/FPA_FOD_20170508/${FilePathBuilder.getDayPattern(prevDaysToIngest)}/in"

  val outputDatabaseName = "bronze_wildfire"
  val outputTableName = "fires"
  val outputDataRelativePath = s"$outputDatabaseName.db/$outputTableName/data"
  val outputCheckpointRelativePath = s"$outputDatabaseName.db/$outputTableName/checkpoint"

  val inputSchema = new StructType()
    .add("OBJECTID", StringType)
    .add("FOD_ID", StringType )
    .add("FPA_ID", StringType)
    .add("SOURCE_SYSTEM_TYPE", StringType)
    .add("SOURCE_SYSTEM", StringType)
    .add("NWCG_REPORTING_AGENCY", StringType)
    .add("NWCG_REPORTING_UNIT_ID", StringType)
    .add("NWCG_REPORTING_UNIT_NAME", StringType)
    .add("SOURCE_REPORTING_UNIT", StringType)
    .add("SOURCE_REPORTING_UNIT_NAME", StringType)
    .add("LOCAL_FIRE_REPORT_ID", StringType)
    .add("LOCAL_INCIDENT_ID", StringType)
    .add("FIRE_CODE", StringType)
    .add("FIRE_NAME", StringType)
    .add("ICS_209_INCIDENT_NUMBER", StringType)
    .add("ICS_209_NAME", StringType)
    .add("MTBS_ID", StringType)
    .add("MTBS_FIRE_NAME", StringType)
    .add("COMPLEX_NAME", StringType)
    .add("FIRE_YEAR", StringType)
    .add("DISCOVERY_DATE", StringType)
    .add("DISCOVERY_DOY", StringType)
    .add("DISCOVERY_TIME", StringType)
    .add("STAT_CAUSE_CODE", StringType)
    .add("STAT_CAUSE_DESCR", StringType)
    .add("CONT_DATE", StringType)
    .add("CONT_DOY", StringType)
    .add("CONT_TIME", StringType)
    .add("FIRE_SIZE", StringType)
    .add("FIRE_SIZE_CLASS", StringType)
    .add("LATITUDE", StringType)
    .add("LONGITUDE", StringType)
    .add("OWNER_CODE", StringType)
    .add("OWNER_DESCR", StringType)
    .add("STATE", StringType)
    .add("COUNTY", StringType)
    .add("FIPS_CODE", StringType)
    .add("FIPS_NAME", StringType)
    .add("Shape", StringType)

  override def execute(): Unit = {
    load(transform(extract()))
  }

  override protected def extract(): DataFrame = {
    spark
      .readStream
      .option("sep", "\t")
      .option("header", "true")
      .schema(inputSchema)
      .csv(inputPath)
  }

  override protected def transform(extractedDf: DataFrame): DataFrame = {
    extractedDf
      .withColumn("ExtractionDate", getExtractionDate(input_file_name()))
  }

  override protected def load(transformedDf: DataFrame): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $outputDatabaseName")

    transformedDf
      .writeStream
      .trigger(Trigger.AvailableNow())
      .outputMode("append")
      .partitionBy("ExtractionDate")
      .format("delta")
      .option("path", s"$outputDataRelativePath")
      .option("checkpointLocation", s"$curatedZonePath/$outputCheckpointRelativePath")
      .toTable(s"$outputDatabaseName.$outputTableName")
      .awaitTermination()

    DeltaTable.forName(spark,s"$outputDatabaseName.$outputTableName").optimize().executeCompaction()
    DeltaTable.forName(spark,s"$outputDatabaseName.$outputTableName").vacuum()
  }

}
