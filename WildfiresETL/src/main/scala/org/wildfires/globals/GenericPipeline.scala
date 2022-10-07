package org.wildfires.globals

import org.apache.spark.sql.DataFrame

trait GenericPipeline {

  def execute(): Unit

  protected def extract(): DataFrame

  protected def transform(extractedDf: DataFrame): DataFrame

  protected def load(transformedDf: DataFrame): Unit

}
