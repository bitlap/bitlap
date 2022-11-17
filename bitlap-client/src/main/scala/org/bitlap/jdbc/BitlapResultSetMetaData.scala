/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.network.models.TypeId

import java.sql._

/** bitlap 结果集的元数据
 *  @author
 *    梦境迷离
 *  @since 2021/6/12
 *  @version 1.0
 */
class BitlapResultSetMetaData(
  private val columnNames: List[String],
  private val columnTypes: List[String]
) extends ResultSetMetaData {

  override def getColumnCount(): Int = columnNames.size

  override def isAutoIncrement(column: Int): Boolean = false

  override def isCurrency(column: Int): Boolean = false

  override def isNullable(column: Int): Int = ResultSetMetaData.columnNullable

  override def getColumnDisplaySize(column: Int): Int =
    // taking a stab at appropriate values
    getColumnType(column) match {
      case Types.VARCHAR | Types.BIGINT               => 32
      case Types.TINYINT                              => 1
      case Types.SMALLINT                             => 2
      case Types.BOOLEAN                              => 8
      case Types.DOUBLE | Types.INTEGER | Types.FLOAT => 16
      case _                                          => 32
    }

  override def getColumnLabel(column: Int): String = columnNames(toZeroIndex(column))

  override def getColumnName(column: Int): String = columnNames(toZeroIndex(column))

  override def getSchemaName(column: Int): String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getPrecision(column: Int): Int =
    if (Types.DOUBLE == getColumnType(column)) -1 else 0 // Do we have a precision limit?

  override def getScale(column: Int): Int =
    if (Types.DOUBLE == getColumnType(column)) -1 else 0 // Do we have a scale limit?

  override def getTableName(column: Int): String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getCatalogName(column: Int): String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getColumnType(column: Int): Int = {
    val typ        = columnTypes(toZeroIndex(column))
    val bitlapType = TypeId.values.find(_.name == typ)
    if (bitlapType.isEmpty || !TypeId.bitlap2Jdbc.contains(bitlapType.getOrElse(TypeId.Unspecified)))
      throw BitlapSQLException("Could not determine column type name for ResultSet")
    TypeId.bitlap2Jdbc(TypeId.values.find(_.name == typ).getOrElse(TypeId.Unspecified))
  }

  override def getColumnTypeName(column: Int): String = columnTypes(toZeroIndex(column))

  override def isCaseSensitive(column: Int): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isSearchable(column: Int): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isSigned(column: Int): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isReadOnly(column: Int): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isWritable(column: Int): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isDefinitelyWritable(column: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getColumnClassName(column: Int): String = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def unwrap[T](iface: Class[T]): T = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isWrapperFor(iface: Class[_]): Boolean =
    throw new SQLFeatureNotSupportedException("Method not supported")

  protected def toZeroIndex(column: Int): Int = {
    if (columnTypes == null) throw BitlapSQLException("Could not determine column type name for ResultSet")
    if (column < 1 || column > columnTypes.size) throw BitlapSQLException("Invalid column value: " + column)
    column - 1
  }
}
