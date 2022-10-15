package suites.org.wildfires.etl.bronze.utils

import org.scalatest.funsuite.AnyFunSuite
import org.wildfires.etl.bronze.wildfire.utils.Functions._

class Bronze_Utils_Functions extends AnyFunSuite {

  test("getExtractionDate()") {
    assert(_getExtractionDate("/storage/raw/FPA_FOD_20170508/2022-09-12/in/Fires.csv") == "2022-09-12")
    assertThrows[Exception](_getExtractionDate("fasdfgsdgsfds"))
    assertThrows[Exception](_getExtractionDate(""))
    assertThrows[Exception](_getExtractionDate(null))
  }

}
