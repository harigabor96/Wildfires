package org.wildfires.pipeline

import org.apache.spark.sql.DataFrame

trait GenericPipeline {

  def execute: Unit = {
    load(transform(extract()))
  }

  def extract(): Any

  def transform(extractedData: Any): DataFrame

  def load(transformedData: DataFrame): Unit
}