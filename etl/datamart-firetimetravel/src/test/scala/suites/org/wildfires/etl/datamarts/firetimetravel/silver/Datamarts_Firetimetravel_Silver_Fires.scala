package suites.org.wildfires.etl.datamarts.firetimetravel.silver

import org.scalatest.funsuite.AnyFunSuite
import testutils.SharedSparkSession
import org.apache.spark.sql.functions.col

class Datamarts_Firetimetravel_Silver_Fires extends AnyFunSuite with SharedSparkSession {

  val sourcePath = "../storage/curated/dm_firetimetravel_silver.db/fires/data"

  //Deduplication Tests
  test("Deduplication") {
    val fires = spark.read.format("delta").load(sourcePath)

    val rowCount = fires.count()
    val distinctRowCount = fires.dropDuplicates().count()

    assert(rowCount == distinctRowCount)
  }

  //Data Cleansing Tests
  test("Data Cleansing - ContDate") {
    val fires = spark.read.format("delta").load(sourcePath)

    val rowCount = fires.count()
    val contDateRowCount = fires
      .filter(
        col("ContDate").isNotNull
      ).count()

    assert(rowCount == contDateRowCount)
  }

  //Filter Criteria Tests
    //Not needed for the current use case

}
