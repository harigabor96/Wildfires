package testutils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, Suite}
import java.io.File

trait SharedTestWarehouse extends BeforeAndAfterEach with SharedSparkSession { self: Suite =>

  val testWarehousePath = "./src/test/resources/Test-Warehouse"
  val testDatabaseName = "commontest"
  val testTableName = "test"
  val testDataRelativePath = s"$testDatabaseName.db/$testTableName/data"

  private def deleteDirRecursive(path: String): Unit = {
    val dir = new File(path)
    val allContents = dir.listFiles()
    if (allContents != null) {
      for (file <- allContents) {
        deleteDirRecursive(file.toPath.toString)
      }
    }
    dir.delete()
  }

  override def beforeAll(): Unit = {
    _spark =
      SparkSession
        .builder()
        .appName("Wildfires")
        .config("spark.sql.warehouse.dir", testWarehousePath)
        .master("local")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    deleteDirRecursive(testWarehousePath)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    val spark = _spark
    import spark.implicits._

    Seq("1")
      .toDF()
      .write
      .format("delta")
      .mode("overwrite")
      .save(s"$testWarehousePath/$testDataRelativePath")

    spark.sql(s"""
      CREATE DATABASE IF NOT EXISTS $testDatabaseName
    """)

    spark.sql(s"""
            CREATE TABLE $testDatabaseName.$testTableName
            USING DELTA
            LOCATION '$testDataRelativePath'
    """)
  }

  override def afterEach(): Unit = {
    deleteDirRecursive(s"$testWarehousePath")

    spark.sql(s"""
      DROP DATABASE $testDatabaseName CASCADE
    """)
  }

}
