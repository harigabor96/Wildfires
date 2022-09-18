package org.wildfires.etl

import org.apache.spark.sql.DataFrame

trait GenericPipeline {

  def execute(): Unit

  def extract(): DataFrame
  
  def transform(extractedDf: DataFrame): DataFrame

  def load(transformedDf: DataFrame): Unit
}
