package suites.org.module.etl.silver.tables.fires

import org.module.etl.silver.tables.fires.Functions._
import org.scalatest.funsuite.AnyFunSuite

class Silver_Fires_Functions extends AnyFunSuite {

  test("_getDate()") {
    assert(_getDate(2022, 365) == "2022-12-31")
    assertThrows[Exception](_getDate(null, 365))
    assertThrows[Exception](_getDate(2022, null))
    assertThrows[Exception](_getDate(2022, 0))
    assertThrows[Exception](_getDate(2022, 400))
  }

}
