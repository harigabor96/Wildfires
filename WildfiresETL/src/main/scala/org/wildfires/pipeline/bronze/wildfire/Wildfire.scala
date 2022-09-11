package org.wildfires.pipeline.bronze.wildfire

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.wildfires.pipeline.GenericPipeline

case class Wildfire(spark: SparkContext) extends GenericPipeline {

  override def extract(): Any = {
    spark.

  }

  override def transform(extractedData: Any): DataFrame = {
    val extractedDf = extractedData.asInstanceOf[DataFrame]
    extractedDf
  }

  override def load(transformedData: DataFrame): Unit = {
    transformedData.show()
  }
}
