package suites.org.module.etl.silver.tables.fires

import org.module.etl.silver.tables.fires.Functions._
import org.scalatest.funsuite.AnyFunSuite

class Silver_Fires_Functions extends AnyFunSuite {

  test("_get_date()") {
    assert(_get_date(2022, 365) == "2022-12-31")
    assertThrows[Exception](_get_date(null, 365))
    assertThrows[Exception](_get_date(2022, null))
    assertThrows[Exception](_get_date(2022, 0))
    assertThrows[Exception](_get_date(2022, 400))
  }

}
