/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.spark3

import org.apache.spark.sql.{ SaveMode, SparkSession }

/** TODO (unit tests)
 */
object Tests {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*,2]")
      .getOrCreate()

    val path = Tests.getClass.getClassLoader.getResource("simple_data.csv").getPath

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(path)
      .selectExpr(
        "cast(time as bigint) as time",
        "entity",
        "dimensions",
        "metric_name",
        "cast(metric_value as double) as metric_value"
      )
    df.show(false)

    df.write
      .format("bitlap")
      .option("url", "jdbc:bitlap://localhost:23333")
      .option("dbtable", "xxx")
      .mode(SaveMode.Overwrite)
      .save()
  }
}
