package suites.org.module.etl.bronze.tables.fires

import org.scalatest.funsuite.AnyFunSuite
import utils.SharedSparkSession

class Bronze_Fires_Pipeline extends AnyFunSuite with SharedSparkSession {
  
  val sourcePath = "../../storage/curated/bronze_wildfire.db/fires/data"

  //Schema Validation & Evolution Tests
    //Not needed for the current use case

  //Ingestion Tests
  test("Ingested Row Count") {
    val fires = spark.read.format("delta").load(sourcePath)

    val ingestedRowCount = fires.count()

    //Extracted manually from the raw CSVs
    val rawRowCount = 6011 * 2

    assert(ingestedRowCount == rawRowCount)
  }

}
