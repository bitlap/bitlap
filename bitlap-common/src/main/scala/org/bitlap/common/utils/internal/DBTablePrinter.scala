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
package org.bitlap.common.utils.internal

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Types

import scala.collection.mutable.ListBuffer

import org.bitlap.common.extension._
import org.bitlap.common.utils.PreConditions

/** Java ResultSet Printer, refer to [[https://github.com/htorun/dbtableprinter]]
 */
object DBTablePrinter {

  /** Default maximum width for text columns
   */
  private val DEFAULT_MAX_TEXT_COL_WIDTH = 150

  /** Default maximum number of rows to query and print.
   */
  private val DEFAULT_MAX_ROWS = 10

  /** Column type category for `CHAR`, `VARCHAR` and similar text columns.
   */
  private val CATEGORY_STRING = 1

  /** Column type category for `TINYINT`, `SMALLINT`, `INT` and `BIGINT` columns.
   */
  private val CATEGORY_INTEGER = 2

  /** Column type category for `REAL`, `DOUBLE`, and `DECIMAL` columns.
   */
  private val CATEGORY_DOUBLE = 3

  /** Column type category for date and time related columns like `DATE`, `TIME`, `TIMESTAMP` etc.
   */
  private val CATEGORY_DATETIME = 4

  /** Column type category for `BOOLEAN` columns.
   */
  private val CATEGORY_BOOLEAN = 5

  /** Column type category for types for which the type name will be printed instead of the content, like `BLOB`,
   *  `BINARY`, `ARRAY` etc.
   */
  private val CATEGORY_OTHER = 0

  case class Column(
    label: String,
    `type`: Int, // Generic SQL type of the column as defined in java.sql.Types.
    typeName: String,
    var width: Int = 0, // Width of the column that will be adjusted according to column label and values to be printed.
    values: ListBuffer[String] = ListBuffer(), // Column values from each row of a ResultSet.
    typeValues: ListBuffer[Any] = ListBuffer(),
    var justifyFlag: String =
      "", // Flag for text justification. Empty string "" to justify right, dash - to justify left.
    var typeCategory: Int =
      0 // Column type category. The columns will be categorised according to their column types and specific needs to print them correctly.
  ) {

    def justifyLeft(): Unit = {
      this.justifyFlag = "-"
    }
  }

  def from(rs: ResultSet, maxStringColWidth: Int = DEFAULT_MAX_TEXT_COL_WIDTH): DBTable = {
    PreConditions.checkNotNull(rs)
    PreConditions.checkExpression(!rs.isClosed, msg = s"Result Set is closed!")

    // Get the meta data object of this ResultSet.
    val rsmd = rs.getMetaData

    // Total number of columns in this ResultSet
    val columnCount = rsmd.getColumnCount

    // List of Column objects to store each columns of the ResultSet and the String representation of their values.
    val columns = ListBuffer[Column]()

    // List of table names. Can be more than one if it is a joined table query
    val tableNames = ListBuffer[String]()

    // Get the columns and their metadata.
    // NOTE: columnIndex for rsmd.getXXX methods STARTS AT 1 NOT 0
    for (i <- 1 to columnCount) {
      val c = Column(rsmd.getColumnLabel(i), rsmd.getColumnType(i), rsmd.getColumnTypeName(i))
      c.width = c.label.length
      c.typeCategory = whichCategory(c.`type`)
      columns += c
      if (!tableNames.contains(rsmd.getTableName(i))) {
        tableNames += rsmd.getTableName(i)
      }
    }

    // Go through each row, get values of each column and adjust
    // column widths.
    var rowCount = 0
    while (rs.next()) {

      // NOTE: columnIndex for rs.getXXX methods STARTS AT 1 NOT 0
      for (i <- 0 until columnCount) {
        val c = columns(i)
        var value = if (c.typeCategory == CATEGORY_OTHER) {
          // Use generic SQL type name instead of the actual value for column types BLOB, BINARY etc.
          "(" + c.typeName + ")"
        } else {
          if (rs.getObject(i + 1) == null) "NULL" else rs.getObject(i + 1).toString
        }
        c.typeCategory match {
          case CATEGORY_DOUBLE =>
            // For real numbers, format the string value to have 3 digits
            // after the point. THIS IS TOTALLY ARBITRARY and can be
            // improved to be CONFIGURABLE.
            if (value != "NULL") {
              val dValue = rs.getDouble(i + 1)
              value = String.format("%.3f", dValue)
            }

          case CATEGORY_STRING =>
            // Left justify the text columns
            c.justifyLeft()

            // and apply the width limit
            if (value.length > maxStringColWidth) {
              value = value.substring(0, maxStringColWidth - 3) + "..."
            }
          case _ =>
        }
        // Adjust the column width
        c.width = if (value.length > c.width) value.length else c.width
        c.values += value
        c.typeValues += rs.getObject(i + 1)
      }
      rowCount += 1
    }
    DBTable(tableNames.toList, if (tableNames.isEmpty && columns.isEmpty) 0 else rowCount, columns.toList)
  }

  def show(table: DBTable): Unit = {
    if (table == null) {
      return
    }
    val tableNames = table.tableNames
    val rowCount   = table.rowCount
    val columns    = table.columns

    /*
     * At this point we have gone through meta data, get the
     * columns and created all Column objects, iterated over the
     * ResultSet rows, populated the column values and adjusted
     * the column widths.
     * We cannot start printing just yet because we have to prepare
     * a row separator String.
     */

    // For the fun of it, I will use StringBuilder
    val strToPrint   = StringBuilder()
    val rowSeparator = StringBuilder()

    /*
     * Prepare column labels to print as well as the row separator.
     * It should look something like this:
     * +--------+------------+------------+-----------+  (row separator)
     * | EMP_NO | BIRTH_DATE | FIRST_NAME | LAST_NAME |  (labels row)
     * +--------+------------+------------+-----------+  (row separator)
     */

    columns.foreach { c =>
      var width = c.width

      // Center the column label
      val name: String = c.label
      var diff         = width - name.length

      if (diff % 2 == 1) {
        // diff is not divisible by 2, add 1 to width (and diff)
        // so that we can have equal padding to the left and right
        // of the column label.
        width += 1
        diff += 1
        c.width = width
      }
      val paddingSize = diff / 2 // InteliJ says casting to int is redundant.

      // Cool String repeater code thanks to user102008 at stackoverflow.com
      // (http://tinyurl.com/7x9qtyg) "Simple way to repeat a string in java"
      val padding = String(new Array[Char](paddingSize)).replace("\u0000", " ")

      val toPrint = s"| $padding$name$padding "
      // END centering the column label

      strToPrint.append(toPrint)
      rowSeparator.append("+")
      rowSeparator.append(String(new Array[Char](width + 2)).replace("\u0000", "-"))
    }
    var lineSeparator = System.getProperty("line.separator")

    // Is this really necessary ??
    lineSeparator = Option(lineSeparator).getOrElse("\n")

    rowSeparator.append("+").append(lineSeparator)

    strToPrint.append("|").append(lineSeparator)
    strToPrint.insert(0, rowSeparator)
    strToPrint.append(rowSeparator)

    val sj = tableNames.mkString(", ")

    var info = s"Printing $rowCount"
    info = info + (if (rowCount > 1) " rows from " else " row from ")
    info = info + (if (tableNames.size > 1) "tables " else "table ")
    info = info + sj

    println(info)

    // Print out the formatted column labels
    print(strToPrint.toString())

    var format: String = ""
    for (i <- 0 until rowCount) {
      columns.foreach { c =>
        // This should form a format string like: "%-60s"
        format = String.format(s"| %%%s%ds ", c.justifyFlag, c.width)
        print(String.format(format, c.values(i)))
      }
      println("|")
      print(rowSeparator)
    }
    println()

    /*
     *   Hopefully this should have printed something like this:
     *   +--------+------------+------------+-----------+--------+-------------+
     *   | EMP_NO | BIRTH_DATE | FIRST_NAME | LAST_NAME | GENDER |  HIRE_DATE  |
     *   +--------+------------+------------+-----------+--------+-------------+
     *   |  10001 | 1953-09-02 | Georgi     | Facello   | M      |  1986-06-26 |
     *   +--------+------------+------------+-----------+--------+-------------+
     *   |  10002 | 1964-06-02 | Bezalel    | Simmel    | F      |  1985-11-21 |
     *   +--------+------------+------------+-----------+--------+-------------+
     */
  }

  /** print from jdbc connection and table name.
   */
  def show(
    conn: Connection,
    tableName: String,
    maxRows: Int = DEFAULT_MAX_ROWS,
    maxStringColWidth: Int = DEFAULT_MAX_TEXT_COL_WIDTH
  ): Unit = {
    PreConditions.checkNotNull(conn)
    PreConditions.checkNotBlank(tableName, "tableName")
    PreConditions.checkExpression(!conn.isClosed, msg = "Connection is closed!")
    val sqlSelectAll = s"SELECT * FROM $tableName LIMIT $maxRows"

    conn.createStatement().use { stmt =>
      val rs = stmt.executeQuery(sqlSelectAll)
      show(from(rs, maxStringColWidth))
      rs.use { _ => }
    }
  }

  /** Takes a generic SQL type and returns the category this type belongs to. Types are categorized according to print
   *  formatting needs:
   *
   *  Integers should not be truncated so column widths should be adjusted without a column width limit. Text columns
   *  should be left justified and can be truncated to a max. column width etc...
   */
  private def whichCategory(`type`: Int): Int = {
    `type` match {
      case Types.BIGINT | Types.TINYINT | Types.SMALLINT | Types.INTEGER =>
        CATEGORY_INTEGER

      case Types.REAL | Types.DOUBLE | Types.DECIMAL =>
        CATEGORY_DOUBLE

      case Types.DATE | Types.TIME | Types.TIME_WITH_TIMEZONE | Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
        CATEGORY_DATETIME

      case Types.BOOLEAN =>
        CATEGORY_BOOLEAN

      case Types.VARCHAR | Types.NVARCHAR | Types.LONGVARCHAR | Types.LONGNVARCHAR | Types.CHAR | Types.NCHAR =>
        CATEGORY_STRING

      case _ => CATEGORY_OTHER
    }
  }

  def normalizeValue(columnType: Int, columnTypeName: String, _value: Any): String = {
    val category = DBTablePrinter.whichCategory(columnType)
    val value = if (category == CATEGORY_OTHER) {
      "(" + columnTypeName + ")"
    } else {
      if (_value == null) "NULL" else _value
    }
    val __Value = _value.toString
    category match {
      case CATEGORY_DOUBLE if value != "NULL" =>
        val dValue = __Value.toDoubleOption.getOrElse(0d)
        String.format("%.3f", dValue)
      case CATEGORY_STRING if __Value.length > DEFAULT_MAX_TEXT_COL_WIDTH =>
        __Value.substring(0, DEFAULT_MAX_TEXT_COL_WIDTH - 3) + "..."
      case _ => __Value
    }
  }
}
