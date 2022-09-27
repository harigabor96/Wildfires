package suites.org.wildfires.etl.datamarts.firetimetravel.utils

import org.scalatest._
import org.wildfires.etl.datamarts.firetimetravel.utils.Functions._
import testutils.SharedSparkSession

class Datamarts_Firetimetravel_Utils_Functions extends FunSuite with SharedSparkSession {

  test("_getDate()") {
    assert(_getDate(2022, 365) == "2022-12-31")
    assertThrows[Exception](_getDate(null, 365))
    assertThrows[Exception](_getDate(2022, null))
    assertThrows[Exception](_getDate(2022, 0))
    assertThrows[Exception](_getDate(2022, 400))
  }

  test("_daysFromInterval()") {
    assert(_daysFromInterval("2022-01-01", "2022-01-03") == List("2022-01-01", "2022-01-02", "2022-01-03"))
    assert(_daysFromInterval("2022-01-01", "2022-01-01") == List("2022-01-01"))
    assertThrows[Exception](_daysFromInterval(null, null))
    assertThrows[Exception](_daysFromInterval(null, "2022-01-01"))
    assertThrows[Exception](_daysFromInterval("2022-01-01", null))
    assertThrows[Exception](_daysFromInterval("", ""))
    assertThrows[Exception](_daysFromInterval("2022-01-01", ""))
    assertThrows[Exception](_daysFromInterval("", "2022-01-01"))
    assertThrows[Exception](_daysFromInterval("asddasd", "asddadsa"))
    assertThrows[Exception](_daysFromInterval("2022-01-01", "asdassdads"))
    assertThrows[Exception](_daysFromInterval("asdadasad", "2022-01-01"))
  }

}
