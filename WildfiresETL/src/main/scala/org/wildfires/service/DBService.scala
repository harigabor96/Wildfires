package org.wildfires.service

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object DBService {

  def createDatabaseIfNotExist(spark: SparkSession, databaseName: String) = {
    spark.sql(s"""
      CREATE DATABASE IF NOT EXISTS $databaseName
    """)
  }

  def createDeltaTableFromPath(spark: SparkSession, databaseName: String, tableName: String, deltaLocation: String) = {
    spark.sql(s"""
            CREATE TABLE $databaseName.$tableName
            USING DELTA
            LOCATION '$deltaLocation'
    """)
  }

  def optimizeTable(spark: SparkSession, databaseName: String, tableName: String) = {
    DeltaTable.forName(spark,s"$databaseName.$tableName").optimize().executeCompaction()
  }

  def vacuumTable(spark: SparkSession, databaseName: String, tableName: String) = {
    DeltaTable.forName(spark,s"$databaseName.$tableName").vacuum()
  }
}
