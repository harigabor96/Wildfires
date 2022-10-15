package suites.org.module.etl.goldtables.arch_fireday

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import utils.SharedSparkSession

class Gold_Arch_FireDay_Pipeline extends AnyFunSuite with SharedSparkSession {

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
