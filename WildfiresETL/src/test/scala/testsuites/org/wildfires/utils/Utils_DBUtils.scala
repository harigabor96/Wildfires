package testsuites.org.wildfires.utils

import org.apache.spark.sql.DataFrame
import testutils._
import org.scalatest._
import org.wildfires.utils.DBUtils._

class Utils_DBUtils extends FunSuite with SharedSparkSession {

  test("createDatabaseIfNotExist()") {
    assertThrows[Exception](createDatabaseIfNotExist(spark,null))
    assertThrows[Exception](createDatabaseIfNotExist(spark,""))
    assertThrows[Exception](createDatabaseIfNotExist(null,"database"))
  }

  test("createTableFromPath()") {
    assertThrows[Exception](createTableFromPath(null, "database", "table", "delta"))
    assertThrows[Exception](createTableFromPath(spark, null, "table", "delta"))
    assertThrows[Exception](createTableFromPath(spark, "database", null, "delta"))
    assertThrows[Exception](createTableFromPath(spark, "database", "table", null))
    assertThrows[Exception](createTableFromPath(spark, "", "table", "delta"))
    assertThrows[Exception](createTableFromPath(spark, "database", "", "delta"))
    assertThrows[Exception](createTableFromPath(spark, "database", "table", ""))
  }

  test("optimizeTable()") {
    assertThrows[Exception](optimizeTable(null, "database", "table"))
    assertThrows[Exception](optimizeTable(spark, null, "table"))
    assertThrows[Exception](optimizeTable(spark, "database", null))
    assertThrows[Exception](optimizeTable(spark, "", "table"))
    assertThrows[Exception](optimizeTable(spark, "database", ""))
  }

  test("vacuumTable()") {
    assertThrows[Exception](vacuumTable(null, "database", "table"))
    assertThrows[Exception](vacuumTable(spark, null, "table"))
    assertThrows[Exception](vacuumTable(spark, "database", null))
    assertThrows[Exception](vacuumTable(spark, "", "table"))
    assertThrows[Exception](vacuumTable(spark, "database", ""))
  }
}
