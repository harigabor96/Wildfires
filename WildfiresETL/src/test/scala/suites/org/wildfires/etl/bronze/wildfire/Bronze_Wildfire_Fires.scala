package suites.org.wildfires.etl.bronze.wildfire

import org.scalatest._
import testutils.SharedSparkSession

class Bronze_Wildfire_Fires extends FunSuite with SharedSparkSession {
  
  val sourcePath = "../storage/curated/bronze_wildfire.db/fires/data"

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
