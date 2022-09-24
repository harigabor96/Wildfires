package testsuites.org.wildfires.etl.datamarts.firetimetravel.silver

import org.apache.spark.sql.functions.col
import testutils._
import org.scalatest._

class Datamarts_Firetimetravel_Silver_Fires extends FunSuite with SharedSparkSession {

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
    //Not needed in the current use case
}
