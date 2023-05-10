package org.bitlap.spark

import org.apache.spark.sql.types._

/** Desc: spark utils
 */
object SparkUtils {

  val VALID_INPUT_SCHEMA: StructType = StructType(
    List(
      StructField("time", LongType),
      StructField("entity", IntegerType),
      StructField("dimensions", DataTypes.createMapType(StringType, StringType)),
      StructField("metric_name", StringType),
      StructField("metric_value", DoubleType)
    )
  )
}
