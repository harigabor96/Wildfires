package testutils

import org.apache.commons.io.FileUtils.deleteDirectory
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, Suite}

import java.io.File

trait WarehouseTest extends SharedSparkSession with BeforeAndAfterEach { self: Suite =>

  val testWarehousePath = "./test/resources/Test-Warehouse"
  val testDatabaseName = "commontest"
  val testTableName = "test"
  val outputDataRelativePath = s"$testDatabaseName.db/$testTableName/data"

  private def deleteDirRecursive(path: String): Unit = {
    val dir = new File(path)
    val allContents = dir.listFiles()
    if (allContents != null) {
      for (file <- allContents) {
        deleteDirectory(file)
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

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    deleteDirRecursive(testWarehousePath)
    _spark.stop()
    _spark = null
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    val spark = _spark
    import spark.implicits._

    Seq("1")
      .toDF()
      .write
      .partitionBy("DiscoveryDate")
      .format("delta")
      .mode("overwrite")
      .save(s"$testWarehousePath/$outputDataRelativePath")

    super.beforeEach()
  }

  override def afterEach(): Unit = {
    deleteDirRecursive(s"$testWarehousePath/$outputDataRelativePath")
    super.beforeEach()
  }
}
