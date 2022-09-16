package org.wildfires.etl.datamart.firetimetravel.util

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import java.time.Year

object Functions {

  def _getDate(year: Int , doy: Int): String = {
    Year.of(year)
      .atDay(doy)
      .toString
  }
  def getDate: UserDefinedFunction = udf(_getDate _)

}
