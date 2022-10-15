package suites.org.module.etl.bronzetables.fires

import org.module.etl.bronzetables.fires.Functions._
import org.scalatest.funsuite.AnyFunSuite

class Fires_Functions extends AnyFunSuite {

  test("getExtractionDate()") {
    assert(_getExtractionDate("/storage/raw/FPA_FOD_20170508/2022-09-12/in/Fires.csv") == "2022-09-12")
    assertThrows[Exception](_getExtractionDate("fasdfgsdgsfds"))
    assertThrows[Exception](_getExtractionDate(""))
    assertThrows[Exception](_getExtractionDate(null))
  }

}
