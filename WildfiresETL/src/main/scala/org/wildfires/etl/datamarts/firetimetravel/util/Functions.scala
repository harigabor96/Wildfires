package org.wildfires.etl.datamarts.firetimetravel.util

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.time.Year
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ListBuffer

object Functions {

  def _getDate(year: Int , doy: Int): String = {
    Year.of(year)
      .atDay(doy)
      .toString
  }
  def getDate: UserDefinedFunction = udf(_getDate _)

  def _daysFromInterval(startDateString: String , endDateString: String): List[String] = {

    val startDate = LocalDate.parse(startDateString)
    val endDate = LocalDate.parse(endDateString)

    val numOfDaysBetween = ChronoUnit.DAYS.between(startDate, endDate).toInt
    var daysBuffer = new ListBuffer[String]

    for( i <- 0 to numOfDaysBetween ) {
      daysBuffer += startDate.plusDays(i).toString
    }

    daysBuffer.toList
  }
  def daysFromInterval: UserDefinedFunction = udf(_daysFromInterval _)
}
