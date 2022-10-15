package suites.org.wildfires.etl.datamarts.firetimetravel.gold

import org.scalatest.funsuite.AnyFunSuite
import testutils.SharedSparkSession
import org.apache.spark.sql.functions.col

class Datamarts_Firetimetravel_Gold_Arch_FireDay extends AnyFunSuite with SharedSparkSession {

  val silverSourcePath = "../storage/curated/dm_firetimetravel_silver.db/fires/data"
  val goldSourcePath = "../storage/curated/dm_firetimetravel_gold.db/arch_fireday/data"

  //Filter Criteria Silver Integration Tests
    //Not needed for the current use case

  //Explode Silver Integration Tests
  test("Data Cleansing - ContDate") {
    val silver = spark.read.format("delta").load(silverSourcePath)
    val gold = spark.read.format("delta").load(goldSourcePath)

    val silverRowCount = silver.count()

    val goldDistinctRowCount =
      gold
        .drop(col("Date"))
        .dropDuplicates()
        .count()

    assert(silverRowCount == goldDistinctRowCount)
  }

}
