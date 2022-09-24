package unit.org.wildfires.etl.bronze.wildfire

import org.apache.spark.sql.functions.col
import testUtil._
import org.scalatest._

class BronzeWildfireFiresSuite extends FunSuite with SharedSparkSession {

  val table =
    spark
      .read
      .format("delta")
      .load("../storage/curated/bronze.wildfires.db/fires/data")

  test("Ingested Row Count") {
    val ingestedRowCount =
      table
        .count()

    //Comes from the raw CSVs
    val rawRowCount = 6011 * 2

    assert(ingestedRowCount == rawRowCount)
  }
}