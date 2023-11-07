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
package org.bitlap.spark

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.matching.Regex

import org.bitlap.common.data.Event

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types._

/** spark utils
 */
object SparkUtils {

  /** get spark valid input schema
   */
  def validSchema: StructType = {
    val fields = Event.schema.map { p =>
      val typ = p._2 match {
        case "int" | "integer" => IntegerType
        case "long" | "bigint" => LongType
        case "double"          => DoubleType
        case "string"          => StringType
        case _                 => throw new IllegalArgumentException("Unknown type: " + p._2)
      }
      StructField(p._1, typ)
    }
    StructType(fields.toList)
  }

  private val dbRegex      = "(.*)(\\.)(.*)".r
  private val regex: Regex = "(.*)".r

  /** get hive table
   */
  def getHiveTableIdentifier(tableName: String): TableIdentifier = {
    tableName match {
      case dbRegex(db, name) => new TableIdentifier(name, Some(db))
      case regex(name)       => new TableIdentifier(name, None)
      case _                 => throw new IllegalArgumentException(s"Illegal targetTableName=[$tableName]")
    }
  }

  /** make dataframe as a sql view to execute with `func`
   */
  def withTempView[T](dfs: (DataFrame, String)*)(func: => T): T = {
    try {
      dfs.foreach { case (df, viewName) =>
        df.createOrReplaceTempView(viewName)
      }
      func
    } finally {
      dfs.foreach { case (df, viewName) =>
        df.sparkSession.catalog.dropTempView(viewName)
      }
    }
  }
}
