package org.wildfires.etl

import org.apache.spark.sql.DataFrame

trait GenericPipeline {

  def execute(): Unit

  def extract(): DataFrame

  def transform(extractedData: DataFrame): DataFrame

  def load(transformedData: DataFrame): Unit
}
