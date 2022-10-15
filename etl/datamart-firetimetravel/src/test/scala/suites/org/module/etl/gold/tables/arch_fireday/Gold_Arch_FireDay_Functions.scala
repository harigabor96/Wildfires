package suites.org.module.etl.gold.tables.arch_fireday

import org.module.etl.gold.tables.arch_fireday.Functions._
import org.scalatest.funsuite.AnyFunSuite

class Gold_Arch_FireDay_Functions extends AnyFunSuite {

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
