package testsuites.org.wildfires.etl.bronze.utils

import org.scalatest._
import testutils._
import org.wildfires.etl.bronze.wildfire.utils.Functions._

class Bronze_Utils_Functions extends FunSuite with SharedSparkSession {

  test("getExtractionDate()") {
    assert(_getExtractionDate("/storage/raw/FPA_FOD_20170508/2022-09-12/in/Fires.csv") == "2022-09-12")
    assertThrows[Exception](_getExtractionDate("fasdfgsdgsfds"))
    assertThrows[Exception](_getExtractionDate(""))
    assertThrows[Exception](_getExtractionDate(null))
  }

}
