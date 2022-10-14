package org.wildfires.etl.bronze.wildfire.utils

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ListBuffer

object FilePathBuilder {

  def getDayPattern(previousDaysOpt: Option[Int]): String = {
    val previousDays = previousDaysOpt.getOrElse(return "{*}")

    if (previousDays < 0) throw new Exception("Previous day count must be 0 or greater!")

    val today = LocalDate.now()

    s"{${getDateRange(today.minusDays(previousDays), today).mkString(",")}}"
  }

  private def getDateRange(startDate: LocalDate, endDate: LocalDate): List[String] = {

    val numOfDaysBetween = ChronoUnit.DAYS.between(startDate, endDate).toInt
    val daysBuffer = new ListBuffer[String]

    for (i <- 0 to numOfDaysBetween) {
      daysBuffer += startDate.plusDays(i).toString
    }

    daysBuffer.toList
  }

}
