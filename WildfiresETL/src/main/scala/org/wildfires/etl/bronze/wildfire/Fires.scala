package org.wildfires.etl.bronze.wildfire

import org.apache.spark.sql.DataFrame
import org.wildfires.etl.{GenericPipeline}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.wildfires.util.DBUtils
import org.wildfires.etl.bronze.wildfire.util.Functions._

case class Fires(spark: SparkSession, rawZonePath: String, curatedZonePath: String) extends GenericPipeline {

  val inputPath = s"$rawZonePath/FPA_FOD_20170508/{*}/in"

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

  override def extract(): DataFrame = {
    spark
      .readStream
      .option("sep", "\t")
      .option("header", "true")
      .schema(inputSchema)
      .csv(inputPath)
  }

  override def transform(extractedDf: DataFrame): DataFrame = {
    extractedDf
      .withColumn("ExtractionDate", getExtractionDate(input_file_name()))
  }

  override def load(transformedDf: DataFrame): Unit = {
    DBUtils.createDatabaseIfNotExist(spark,s"$outputDatabaseName")

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

    DBUtils.optimizeTable(spark, outputDatabaseName, outputTableName)
    DBUtils.vacuumTable(spark, outputDatabaseName, outputTableName)
  }
}
