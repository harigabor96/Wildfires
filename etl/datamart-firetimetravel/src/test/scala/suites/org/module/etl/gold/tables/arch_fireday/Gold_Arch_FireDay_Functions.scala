package suites.org.module.etl.gold.tables.arch_fireday

import org.module.etl.gold.tables.arch_fireday.Functions._
import org.scalatest.funsuite.AnyFunSuite

class Gold_Arch_FireDay_Functions extends AnyFunSuite {

  test("_days_from_interval()") {
    assert(_days_from_interval("2022-01-01", "2022-01-03") == List("2022-01-01", "2022-01-02", "2022-01-03"))
    assert(_days_from_interval("2022-01-01", "2022-01-01") == List("2022-01-01"))
    assertThrows[Exception](_days_from_interval(null, null))
    assertThrows[Exception](_days_from_interval(null, "2022-01-01"))
    assertThrows[Exception](_days_from_interval("2022-01-01", null))
    assertThrows[Exception](_days_from_interval("", ""))
    assertThrows[Exception](_days_from_interval("2022-01-01", ""))
    assertThrows[Exception](_days_from_interval("", "2022-01-01"))
    assertThrows[Exception](_days_from_interval("asddasd", "asddadsa"))
    assertThrows[Exception](_days_from_interval("2022-01-01", "asdassdads"))
    assertThrows[Exception](_days_from_interval("asdadasad", "2022-01-01"))
  }

}
