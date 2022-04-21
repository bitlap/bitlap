/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.network.driver.proto.{ BRow, BTableSchema, BTypeId }

import java.io.{ InputStream, Reader }
import java.math.BigDecimal
import java.net.URL
import java.sql._
import java.time.Instant
import java.util
import java.util.Calendar

/**
 * @author 梦境迷离
 * @since 2021/8/23
 * @version 1.0
 */
abstract class BitlapBaseResultSet extends ResultSet {

  protected var warningChain: SQLWarning
  protected var row: BRow
  protected var columnNames: List[String]
  protected var columnTypes: List[String]

  private var schema: BTableSchema = _

  private var _wasNull: Boolean = false

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???

  override def close(): Unit = ()

  override def wasNull(): Boolean = _wasNull

  override def getString(columnIndex: Int): String = getColumnValue(columnIndex)

  override def getString(columnLabel: String): String = getString(findColumn(columnLabel))

  override def getBoolean(columnIndex: Int): Boolean = getColumnValue(columnIndex)

  override def getBoolean(columnLabel: String): Boolean = getBoolean(findColumn(columnLabel))

  override def getByte(columnIndex: Int): Byte = ???

  override def getByte(columnLabel: String): Byte = ???

  override def getShort(columnIndex: Int): Short = getColumnValue(columnIndex)

  override def getShort(columnLabel: String): Short = getShort(findColumn(columnLabel))

  override def getInt(columnIndex: Int): Int = getColumnValue(columnIndex)

  override def getInt(columnLabel: String): Int = getInt(findColumn(columnLabel))

  override def getLong(columnIndex: Int): Long = getColumnValue(columnIndex)

  override def getLong(columnLabel: String): Long = getLong(findColumn(columnLabel))

  override def getFloat(columnIndex: Int): Float =
    ???

  override def getFloat(columnLabel: String): Float =
    ???

  override def getDouble(columnIndex: Int): Double = getColumnValue(columnIndex)

  override def getDouble(columnLabel: String): Double = getDouble(findColumn(columnLabel))

  override def getBigDecimal(columnIndex: Int, scale: Int): BigDecimal =
    ???

  override def getBigDecimal(columnLabel: String, scale: Int): BigDecimal =
    ???

  override def getBigDecimal(columnIndex: Int): BigDecimal =
    ???

  override def getBigDecimal(columnLabel: String): BigDecimal =
    ???

  override def getBytes(columnIndex: Int): scala.Array[Byte] = ???

  override def getBytes(columnLabel: String): scala.Array[Byte] =
    ???

  override def getDate(columnIndex: Int): Date =
    ???

  override def getDate(columnLabel: String): Date =
    ???

  override def getDate(columnIndex: Int, cal: Calendar): Date =
    ???

  override def getDate(columnLabel: String, cal: Calendar): Date =
    ???

  override def getTime(columnIndex: Int): Time =
    ???

  override def getTime(columnLabel: String): Time =
    ???

  override def getTime(columnIndex: Int, cal: Calendar): Time =
    ???

  override def getTime(columnLabel: String, cal: Calendar): Time =
    ???

  override def getTimestamp(columnIndex: Int): Timestamp = getColumnValue(columnIndex)

  override def getTimestamp(columnLabel: String): Timestamp = getTimestamp(findColumn(columnLabel))

  override def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp =
    ???

  override def getTimestamp(columnLabel: String, cal: Calendar): Timestamp =
    ???

  override def getAsciiStream(columnIndex: Int): InputStream =
    ???

  override def getAsciiStream(columnLabel: String): InputStream =
    ???

  override def getUnicodeStream(columnIndex: Int): InputStream =
    ???

  override def getUnicodeStream(columnLabel: String): InputStream =
    ???

  override def getBinaryStream(columnIndex: Int): InputStream =
    ???

  override def getBinaryStream(columnLabel: String): InputStream =
    ???

  override def getWarnings(): SQLWarning = warningChain

  override def clearWarnings(): Unit = warningChain = null

  override def getCursorName(): String =
    ???

  override def getMetaData(): ResultSetMetaData = new BitlapResultSetMetaData(columnNames, columnTypes)

  override def getObject(columnIndex: Int): Any = getColumnValue(columnIndex)

  override def getObject(columnLabel: String): Any = getObject(findColumn(columnLabel))

  override def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef = ???

  override def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef = ???

  override def getObject[T](columnIndex: Int, `type`: Class[T]): T = ???

  override def getObject[T](columnLabel: String, `type`: Class[T]): T = ???

  override def findColumn(columnLabel: String): Int = {
    val columnIndex = columnNames.indexOf(columnLabel)
    if (columnIndex == -1) {
      throw BSQLException("Bitlap SQL Exception")
    }
    columnIndex + 1
  }

  override def getCharacterStream(columnIndex: Int): Reader =
    ???

  override def getCharacterStream(columnLabel: String): Reader =
    ???

  override def isBeforeFirst(): Boolean =
    ???

  override def isAfterLast(): Boolean =
    ???

  override def isFirst(): Boolean =
    ???

  override def isLast(): Boolean =
    ???

  override def beforeFirst(): Unit =
    ???

  override def afterLast(): Unit =
    ???

  override def first(): Boolean =
    ???

  override def last(): Boolean =
    ???

  override def getRow(): Int =
    ???

  override def absolute(row: Int): Boolean =
    ???

  override def relative(rows: Int): Boolean =
    ???

  override def previous(): Boolean =
    ???

  override def setFetchDirection(direction: Int): Unit =
    ???

  override def getFetchDirection(): Int =
    ???

  override def setFetchSize(rows: Int): Unit = ???

  override def getFetchSize(): Int =
    ???

  override def getType(): Int = ResultSet.TYPE_FORWARD_ONLY

  override def getConcurrency(): Int =
    ???

  override def rowUpdated(): Boolean =
    ???

  override def rowInserted(): Boolean =
    ???

  override def rowDeleted(): Boolean =
    ???

  override def updateNull(columnIndex: Int): Unit =
    ???

  override def updateNull(columnLabel: String): Unit =
    ???

  override def updateBoolean(columnIndex: Int, x: Boolean): Unit =
    ???

  override def updateBoolean(columnLabel: String, x: Boolean): Unit =
    ???

  override def updateByte(columnIndex: Int, x: Byte): Unit =
    ???

  override def updateByte(columnLabel: String, x: Byte): Unit =
    ???

  override def updateShort(columnIndex: Int, x: Short): Unit =
    ???

  override def updateShort(columnLabel: String, x: Short): Unit =
    ???

  override def updateInt(columnIndex: Int, x: Int): Unit =
    ???

  override def updateInt(columnLabel: String, x: Int): Unit =
    ???

  override def updateLong(columnIndex: Int, x: Long): Unit =
    ???

  override def updateLong(columnLabel: String, x: Long): Unit =
    ???

  override def updateFloat(columnIndex: Int, x: Float): Unit =
    ???

  override def updateFloat(columnLabel: String, x: Float): Unit =
    ???

  override def updateDouble(columnIndex: Int, x: Double): Unit =
    ???

  override def updateDouble(columnLabel: String, x: Double): Unit =
    ???

  override def updateBigDecimal(columnIndex: Int, x: BigDecimal): Unit =
    ???

  override def updateBigDecimal(columnLabel: String, x: BigDecimal): Unit =
    ???

  override def updateString(columnIndex: Int, x: String): Unit =
    ???

  override def updateString(columnLabel: String, x: String): Unit =
    ???

  override def updateBytes(columnIndex: Int, x: scala.Array[Byte]): Unit = ???

  override def updateBytes(columnLabel: String, x: scala.Array[Byte]): Unit = ???

  override def updateDate(columnIndex: Int, x: Date): Unit =
    ???

  override def updateDate(columnLabel: String, x: Date): Unit =
    ???

  override def updateTime(columnIndex: Int, x: Time): Unit =
    ???

  override def updateTime(columnLabel: String, x: Time): Unit =
    ???

  override def updateTimestamp(columnIndex: Int, x: Timestamp): Unit =
    ???

  override def updateTimestamp(columnLabel: String, x: Timestamp): Unit =
    ???

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit =
    ???

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit =
    ???

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit =
    ???

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit =
    ???

  override def updateAsciiStream(columnIndex: Int, x: InputStream): Unit =
    ???

  override def updateAsciiStream(columnLabel: String, x: InputStream): Unit =
    ???

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit =
    ???

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit =
    ???

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit =
    ???

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit =
    ???

  override def updateBinaryStream(columnIndex: Int, x: InputStream): Unit =
    ???

  override def updateBinaryStream(columnLabel: String, x: InputStream): Unit =
    ???

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit =
    ???

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Int): Unit =
    ???

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit =
    ???

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit =
    ???

  override def updateCharacterStream(columnIndex: Int, x: Reader): Unit =
    ???

  override def updateCharacterStream(columnLabel: String, reader: Reader): Unit =
    ???

  override def updateObject(columnIndex: Int, x: Any, scaleOrLength: Int): Unit =
    ???

  override def updateObject(columnIndex: Int, x: Any): Unit =
    ???

  override def updateObject(columnLabel: String, x: Any, scaleOrLength: Int): Unit =
    ???

  override def updateObject(columnLabel: String, x: Any): Unit =
    ???

  override def insertRow(): Unit =
    ???

  override def updateRow(): Unit =
    ???

  override def deleteRow(): Unit =
    ???

  override def refreshRow(): Unit =
    ???

  override def cancelRowUpdates(): Unit =
    ???

  override def moveToInsertRow(): Unit =
    ???

  override def moveToCurrentRow(): Unit =
    ???

  override def getStatement(): Statement =
    ???

  override def getRef(columnIndex: Int): Ref =
    ???

  override def getRef(columnLabel: String): Ref =
    ???

  override def getBlob(columnIndex: Int): Blob =
    ???

  override def getBlob(columnLabel: String): Blob =
    ???

  override def getClob(columnIndex: Int): Clob =
    ???

  override def getClob(columnLabel: String): Clob =
    ???

  override def getArray(columnIndex: Int): java.sql.Array =
    ???

  override def getArray(columnLabel: String): java.sql.Array =
    ???

  override def getURL(columnIndex: Int): URL =
    ???

  override def getURL(columnLabel: String): URL =
    ???

  override def updateRef(columnIndex: Int, x: Ref): Unit =
    ???

  override def updateRef(columnLabel: String, x: Ref): Unit =
    ???

  override def updateBlob(columnIndex: Int, x: Blob): Unit =
    ???

  override def updateBlob(columnLabel: String, x: Blob): Unit =
    ???

  override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit =
    ???

  override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit =
    ???

  override def updateBlob(columnIndex: Int, inputStream: InputStream): Unit =
    ???

  override def updateBlob(columnLabel: String, inputStream: InputStream): Unit =
    ???

  override def updateClob(columnIndex: Int, x: Clob): Unit =
    ???

  override def updateClob(columnLabel: String, x: Clob): Unit =
    ???

  override def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit =
    ???

  override def updateClob(columnLabel: String, reader: Reader, length: Long): Unit =
    ???

  override def updateClob(columnIndex: Int, reader: Reader): Unit =
    ???

  override def updateClob(columnLabel: String, reader: Reader): Unit =
    ???

  override def updateArray(columnIndex: Int, x: java.sql.Array): Unit =
    ???

  override def updateArray(columnLabel: String, x: java.sql.Array): Unit =
    ???

  override def getRowId(columnIndex: Int): RowId =
    ???

  override def getRowId(columnLabel: String): RowId =
    ???

  override def updateRowId(columnIndex: Int, x: RowId): Unit =
    ???

  override def updateRowId(columnLabel: String, x: RowId): Unit =
    ???

  override def getHoldability(): Int =
    ???

  override def isClosed(): Boolean =
    ???

  override def updateNString(columnIndex: Int, nString: String): Unit =
    ???

  override def updateNString(columnLabel: String, nString: String): Unit =
    ???

  override def updateNClob(columnIndex: Int, nClob: NClob): Unit =
    ???

  override def updateNClob(columnLabel: String, nClob: NClob): Unit =
    ???

  override def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit =
    ???

  override def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit =
    ???

  override def updateNClob(columnIndex: Int, reader: Reader): Unit =
    ???

  override def updateNClob(columnLabel: String, reader: Reader): Unit =
    ???

  override def getNClob(columnIndex: Int): NClob =
    ???

  override def getNClob(columnLabel: String): NClob =
    ???

  override def getSQLXML(columnIndex: Int): SQLXML =
    ???

  override def getSQLXML(columnLabel: String): SQLXML =
    ???

  override def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit =
    ???

  override def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit =
    ???

  override def getNString(columnIndex: Int): String =
    ???

  override def getNString(columnLabel: String): String =
    ???

  override def getNCharacterStream(columnIndex: Int): Reader =
    ???

  override def getNCharacterStream(columnLabel: String): Reader =
    ???

  override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit =
    ???

  override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit =
    ???

  override def updateNCharacterStream(columnIndex: Int, x: Reader): Unit =
    ???

  override def updateNCharacterStream(columnLabel: String, reader: Reader): Unit = ???

  @inline
  private def getColumnValue[T](columnIndex: Int): T = {
    if (row == null) {
      throw BSQLException("No row found.")
    }
    val colVals = row.colVals
    if (colVals == null) throw BSQLException("RowSet does not contain any columns!")
    if (columnIndex > colVals.size) {
      throw BSQLException("Invalid columnIndex: $columnIndex")
    }

    // In kotlin, We can not use e.g. Int?,Long?,Double? to override the java interface here.
    val bColumnValue = colVals(columnIndex - 1)
    try {
      if (bColumnValue.isEmpty) {
        _wasNull = true
      }

      val valueStr = bColumnValue.toStringUtf8
      val columnType = getSchema().columns(columnIndex - 1).typeDesc
      (
        columnType match {
          case BTypeId.B_TYPE_ID_STRING_TYPE =>
            if (valueStr.isEmpty) "" else valueStr
          case BTypeId.B_TYPE_ID_INT_TYPE =>
            if (valueStr.nonEmpty) Integer.parseInt(valueStr) else null
          case BTypeId.B_TYPE_ID_DOUBLE_TYPE =>
            if (valueStr.nonEmpty) java.lang.Double.parseDouble(valueStr) else null
          case BTypeId.B_TYPE_ID_SHORT_TYPE =>
            if (valueStr.nonEmpty) java.lang.Short.parseShort(valueStr) else null
          case BTypeId.B_TYPE_ID_LONG_TYPE =>
            if (valueStr.nonEmpty) java.lang.Long.parseLong(valueStr) else null
          case BTypeId.B_TYPE_ID_BOOLEAN_TYPE =>
            if (valueStr.nonEmpty) java.lang.Boolean.valueOf(valueStr) else null
          case BTypeId.B_TYPE_ID_TIMESTAMP_TYPE =>
            if (valueStr.nonEmpty) Timestamp.from(Instant.ofEpochMilli(java.lang.Long.parseLong(valueStr)))
            else null
          case _ => throw BSQLException(s"Unrecognized column type:$columnType")
        }
      ).asInstanceOf[T]
    } catch {
      case e: Exception => throw e
    }
  }

  def setSchema(schema: BTableSchema): Unit = this.schema = schema

  def getSchema(): BTableSchema = this.schema
}
