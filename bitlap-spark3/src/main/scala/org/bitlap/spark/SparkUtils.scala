/* Copyright (c) 2023 bitlap.org */
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
    val fields = Event.getSchema.asScala.map { p =>
      val typ = p.getSecond match {
        case "int" | "integer" => IntegerType
        case "long" | "bigint" => LongType
        case "double"          => DoubleType
        case "string"          => StringType
        case _                 => throw new IllegalArgumentException("Unknown type: " + p.getSecond)
      }
      StructField(p.getFirst, typ)
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
