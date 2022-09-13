package org.wildfires.pipeline.bronze.wildfire.util

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Functions {

  private def _getExtractionDate(filePath: String): String = {


  }

  def getExtractionDate: UserDefinedFunction = udf(_getExtractionDate(_))
}
