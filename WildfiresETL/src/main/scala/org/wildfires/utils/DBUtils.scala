package org.wildfires.utils

import org.apache.spark.sql.SparkSession

object DBUtils {

  def createDatabaseIfNotExist(spark: SparkSession, databaseName: String) = {
    spark.sql(s"""
      CREATE DATABASE IF NOT EXISTS $databaseName
    """)
  }
}
