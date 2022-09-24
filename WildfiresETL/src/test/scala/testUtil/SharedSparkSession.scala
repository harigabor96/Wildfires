package testUtil

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkSession extends BeforeAndAfterAll { self: Suite =>

  @transient private var _spark: SparkSession = _

  def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    _spark =
      SparkSession
        .builder()
        .appName("Wildfires")
        .master("local")
        .getOrCreate()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    _spark.stop()
    _spark = null
    super.afterAll()
  }
}