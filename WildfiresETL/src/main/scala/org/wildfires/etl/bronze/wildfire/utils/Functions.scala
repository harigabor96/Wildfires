package org.wildfires.etl.bronze.wildfire.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Functions {

  def _getExtractionDate(filePath: String): String = {
    val regex = ".*/(\\d{4}-\\d{2}-\\d{2})/in/.*".r

    filePath match {
      case regex(date) => date
      case _ => throw new Exception("Date not found in File Path!")
    }
  }
  def getExtractionDate: UserDefinedFunction = udf(_getExtractionDate(_))

}
