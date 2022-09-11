package org.wildfires
import org.apache.spark.sql.SparkSession

object App extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  import spark.implicits._

  Seq("Hello","World").toDF().show()
}
