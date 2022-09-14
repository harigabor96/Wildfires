package org.wildfires.etl

import org.apache.spark.sql.DataFrame

trait GenericPipeline {

  def execute(): Unit

  def extract(): Any

  def transform(extractedData: Any): DataFrame

  def load(transformedData: DataFrame): Unit
}
