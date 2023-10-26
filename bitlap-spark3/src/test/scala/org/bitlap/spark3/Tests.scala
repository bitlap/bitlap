/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
