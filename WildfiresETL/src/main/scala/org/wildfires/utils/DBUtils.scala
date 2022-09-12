package org.wildfires.utils

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object DBUtils {

  def createDatabaseIfNotExist(spark: SparkSession, databaseName: String) = {
    spark.sql(s"""
      CREATE DATABASE IF NOT EXISTS $databaseName
    """)
  }

  def optimizeTable(spark: SparkSession, databaseName: String, tableName: String) = {
    DeltaTable.forName(spark,s"$databaseName.$tableName").optimize().executeCompaction()
  }

  def vacuumTable(spark: SparkSession, databaseName: String, tableName: String) = {
    DeltaTable.forName(spark,s"$databaseName.$tableName").vacuum()
  }
}