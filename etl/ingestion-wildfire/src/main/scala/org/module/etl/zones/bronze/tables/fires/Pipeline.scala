package org.module.etl.zones.bronze.tables.fires

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.eztl.core.etl.GenericPipeline
import org.eztl.ingestion.FilePathHelper._
import org.module.init.Conf

case class Pipeline(spark: SparkSession, conf: Conf) extends GenericPipeline {

  val inputPath = s"${conf.rawZonePath()}/FPA_FOD_20170508/fires/${createDayPattern(conf.ingestPreviousDays.toOption)}/in"

  val outputDatabaseName = "bronze_wildfire"
  val outputTableName = "fires"
  val outputDataRelativePath = s"$outputDatabaseName.db/$outputTableName/data"
  val outputCheckpointRelativePath = s"$outputDatabaseName.db/$outputTableName/checkpoint"

  override def execute(): Unit = {
    load(transform(extract()))
  }

  override protected def extract(): DataFrame = {
    spark
      .readStream
      .option("sep", "\t")
      .option("header", "true")
      .schema(Params.inputSchema)
      .csv(inputPath)
  }

  override protected def transform(extractedDf: DataFrame): DataFrame = {
    extractedDf
      .withColumn("ExtractionDate", get_path_part(2)(input_file_name()))
      .withColumn("FileSource", input_file_name())
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
      .option("checkpointLocation", s"${conf.curatedZonePath()}/$outputCheckpointRelativePath")
      .toTable(s"$outputDatabaseName.$outputTableName")
      .awaitTermination()
  }

}
