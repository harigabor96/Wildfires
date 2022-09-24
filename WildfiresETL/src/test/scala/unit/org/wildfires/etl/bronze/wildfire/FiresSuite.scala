package unit.org.wildfires.etl.bronze.wildfire

import testUtil._
import org.scalatest._

class FiresSuite extends FunSuite with SharedSparkSession {
  
  val sourcePath = "C:/Users/harig/Desktop/Wildfires-1/storage/curated/bronze_wildfire.db/fires/data"
  
  test("Ingested Row Count") {
    val fires = spark.read.format("delta").load(sourcePath)

    val ingestedRowCount = fires.count()

    //Comes from the raw CSVs
    val rawRowCount = 6011 * 2

    assert(ingestedRowCount == rawRowCount)
  }
}