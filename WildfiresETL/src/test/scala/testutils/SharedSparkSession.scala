package testutils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkSession extends BeforeAndAfterAll {
  self: Suite =>

  @transient protected var _spark: SparkSession = _

  def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    _spark =
      SparkSession
        .builder()
        .appName("Wildfires")
        .master("local")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    _spark.stop()
    _spark = null
  }

}
