package org.module.etl.silver.tables.fires

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import java.time.Year

object Functions {

  def _getDate(year: Integer, doy: Integer): String = {
    if (year == null) throw new Exception("Year cannot be null!")
    if (doy == null) throw new Exception("Day of the Year cannot be null!")
    if (doy < 1 || 366 < doy) throw new Exception("Day of the Year out of range!")

    Year.of(year)
      .atDay(doy)
      .toString
  }
  def getDate: UserDefinedFunction = udf(_getDate _)

}
