package org.wildfires.etl.bronzemodules.wildfire.util

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Functions {

  private def _getExtractionDate(filePath: String) = {

    //val regex = ".*/(\\d{4}-\\d{2}-\\d{2})".r //remove / after .* if you think its not needed.
/*
    filePath match {
      case regex(date) => Some(date)
      case _ => None
    }
  */
  }

  def getExtractionDate: UserDefinedFunction = udf(_getExtractionDate(_))
}
