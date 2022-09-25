package testutils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, Suite}

trait WarehouseTest extends SharedSparkSession with BeforeAndAfterEach { self: Suite =>

  override def beforeAll(): Unit = {
    _spark =
      SparkSession
        .builder()
        .appName("Wildfires")
        .config("spark.sql.warehouse.dir", "")
        .master("local")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    super.beforeAll()
  }

  override def beforeEach(): Unit = super.beforeEach()

  override def afterEach(): Unit = super.beforeEach()
}
