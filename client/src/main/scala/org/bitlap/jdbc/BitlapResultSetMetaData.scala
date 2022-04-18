/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import java.sql.{ ResultSetMetaData, Types }

/**
 * @author 梦境迷离
 * @since 2021/6/12
 * @version 1.0
 */
class BitlapResultSetMetaData(
  private val columnNames: List[String],
  private val columnTypes: List[String],
  private val tableName: String = ""
) extends ResultSetMetaData {

  override def getColumnCount(): Int = columnNames.size

  override def isAutoIncrement(column: Int): Boolean = false

  override def isCurrency(column: Int): Boolean = false

  override def isNullable(column: Int): Int = ResultSetMetaData.columnNullable

  override def getColumnDisplaySize(column: Int): Int =
    // taking a stab at appropriate values
    getColumnType(column) match {
      case Types.VARCHAR | Types.BIGINT => 32
      case Types.TINYINT                => 2
      case Types.TINYINT                => 1
      case Types.BOOLEAN                => 8
      case Types.DOUBLE | Types.INTEGER => 16
      case _                            => 32
    }

  override def getColumnLabel(column: Int): String = columnNames(column - 1)

  override def getColumnName(column: Int): String = columnNames(column - 1)

  override def getSchemaName(column: Int): String = ???

  override def getPrecision(column: Int): Int =
    if (Types.DOUBLE == getColumnType(column)) -1 else 0 // Do we have a precision limit?

  override def getScale(column: Int): Int =
    if (Types.DOUBLE == getColumnType(column)) -1 else 0 // Do we have a scale limit?

  override def getTableName(column: Int): String = tableName

  override def getCatalogName(column: Int): String = ???

  override def getColumnType(column: Int): Int = {
    if (columnTypes.isEmpty) throw BSQLException("Could not determine column type name for ResultSet")
    if (column < 1 || column > columnTypes.size) throw BSQLException("Invalid column value: $column")
    val typ = columnTypes(column - 1)
    ColumnTyped(typ).toValue
  }

  override def getColumnTypeName(column: Int): String = {
    if (columnTypes.isEmpty) throw BSQLException("Could not determine column type name for ResultSet")
    if (column < 1 || column > columnTypes.size) throw BSQLException("Invalid column value: $column")
    columnTypes(column - 1)
  }

  override def isCaseSensitive(column: Int): Boolean = ???

  override def isSearchable(column: Int): Boolean = ???

  override def isSigned(column: Int): Boolean = ???

  override def isReadOnly(column: Int): Boolean = ???

  override def isWritable(column: Int): Boolean = ???

  override def isDefinitelyWritable(column: Int): Boolean = ???

  override def getColumnClassName(column: Int): String = ???

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???
}
