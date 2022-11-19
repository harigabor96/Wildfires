package org.module.etl.zones.bronze.tables.fires

import org.apache.spark.sql.types._

object Params {

  val inputSchema = new StructType(Array(
    StructField("OBJECTID", StringType),
    StructField("FOD_ID", StringType),
    StructField("FPA_ID", StringType),
    StructField("SOURCE_SYSTEM_TYPE", StringType),
    StructField("SOURCE_SYSTEM", StringType),
    StructField("NWCG_REPORTING_AGENCY", StringType),
    StructField("NWCG_REPORTING_UNIT_ID", StringType),
    StructField("NWCG_REPORTING_UNIT_NAME", StringType),
    StructField("SOURCE_REPORTING_UNIT", StringType),
    StructField("SOURCE_REPORTING_UNIT_NAME", StringType),
    StructField("LOCAL_FIRE_REPORT_ID", StringType),
    StructField("LOCAL_INCIDENT_ID", StringType),
    StructField("FIRE_CODE", StringType),
    StructField("FIRE_NAME", StringType),
    StructField("ICS_209_INCIDENT_NUMBER", StringType),
    StructField("ICS_209_NAME", StringType),
    StructField("MTBS_ID", StringType),
    StructField("MTBS_FIRE_NAME", StringType),
    StructField("COMPLEX_NAME", StringType),
    StructField("FIRE_YEAR", StringType),
    StructField("DISCOVERY_DATE", StringType),
    StructField("DISCOVERY_DOY", StringType),
    StructField("DISCOVERY_TIME", StringType),
    StructField("STAT_CAUSE_CODE", StringType),
    StructField("STAT_CAUSE_DESCR", StringType),
    StructField("CONT_DATE", StringType),
    StructField("CONT_DOY", StringType),
    StructField("CONT_TIME", StringType),
    StructField("FIRE_SIZE", StringType),
    StructField("FIRE_SIZE_CLASS", StringType),
    StructField("LATITUDE", StringType),
    StructField("LONGITUDE", StringType),
    StructField("OWNER_CODE", StringType),
    StructField("OWNER_DESCR", StringType),
    StructField("STATE", StringType),
    StructField("COUNTY", StringType),
    StructField("FIPS_CODE", StringType),
    StructField("FIPS_NAME", StringType),
    StructField("Shape", StringType)
  ))

}
