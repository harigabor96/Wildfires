package org.module.etl.zones.silver.tables.fires

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import java.time.Year

object Functions {

  def _get_date(year: Integer, doy: Integer): String = {
    if (year == null) throw new Exception("Year cannot be null!")
    if (doy == null) throw new Exception("Day of the Year cannot be null!")
    if (doy < 1 || 366 < doy) throw new Exception("Day of the Year out of range!")

    Year.of(year)
      .atDay(doy)
      .toString
  }
  def get_date: UserDefinedFunction = udf(_get_date _)

}
