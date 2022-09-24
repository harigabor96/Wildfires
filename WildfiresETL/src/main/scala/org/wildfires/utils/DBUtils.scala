package org.wildfires.utils

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object DBUtils {

  def createDatabaseIfNotExist(spark: SparkSession, databaseName: String) = {
    databaseName match {
      case null => throw new Exception("Database Name cannot be null!")
      case "" => throw new Exception("Database Name cannot be empty!")
      case _ =>
    }

    spark.sql(s"""
      CREATE DATABASE IF NOT EXISTS $databaseName
    """)
  }

  def createTableFromPath(spark: SparkSession, databaseName: String, tableName: String, deltaLocation: String) = {
    databaseName match {
      case null => throw new Exception("Database Name cannot be null!")
      case "" => throw new Exception("Database Name cannot be empty!")
      case _ =>
    }

    tableName match {
      case null => throw new Exception("Table Name cannot be null!")
      case "" => throw new Exception("Table Name cannot be empty!")
      case _ =>
    }

    deltaLocation match {
      case null => throw new Exception("Delta location cannot be null!")
      case "" => throw new Exception("Delta location cannot be empty!")
      case _ =>
    }

    spark.sql(s"""
            CREATE TABLE $databaseName.$tableName
            USING DELTA
            LOCATION '$deltaLocation'
    """)
  }

  def optimizeTable(spark: SparkSession, databaseName: String, tableName: String) = {
    databaseName match {
      case null => throw new Exception("Database Name cannot be null!")
      case "" => throw new Exception("Database Name cannot be empty!")
      case _ =>
    }

    tableName match {
      case null => throw new Exception("Table Name cannot be null!")
      case "" => throw new Exception("Table Name cannot be empty!")
      case _ =>
    }

    DeltaTable.forName(spark,s"$databaseName.$tableName").optimize().executeCompaction()
  }

  def vacuumTable(spark: SparkSession, databaseName: String, tableName: String) = {
    databaseName match {
      case null => throw new Exception("Database Name cannot be null!")
      case "" => throw new Exception("Database Name cannot be empty!")
      case _ =>
    }

    tableName match {
      case null => throw new Exception("Table Name cannot be null!")
      case "" => throw new Exception("Table Name cannot be empty!")
      case _ =>
    }

    DeltaTable.forName(spark,s"$databaseName.$tableName").vacuum()
  }
}