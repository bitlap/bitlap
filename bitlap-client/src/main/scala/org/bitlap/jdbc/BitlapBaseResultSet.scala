/* Copyright (c) 2023 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.network.models._
import org.bitlap.network.serde.BitlapSerde

import java.io._
import java.math.BigDecimal
import java.net._
import java.sql._
import java.util
import java.util.Calendar
import scala.annotation._
import scala.reflect._

/** bitlap 抽象结果集
 *  @author
 *    梦境迷离
 *  @since 2021/8/23
 *  @version 1.0
 */
abstract class BitlapBaseResultSet extends ResultSet with BitlapSerde {

  protected var warningChain: SQLWarning
  protected var row: Row
  protected var columnNames: List[String]
  protected var columnTypes: List[String]

  private var schema: TableSchema = _

  private var _wasNull: Boolean = false

  override def unwrap[T](iface: Class[T]): T = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isWrapperFor(iface: Class[_]): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def close(): Unit = ()

  override def wasNull(): Boolean = _wasNull

  override def getString(columnIndex: Int): String = getColumnValue[String](columnIndex)

  override def getString(columnLabel: String): String = getString(findColumn(columnLabel))

  override def getBoolean(columnIndex: Int): Boolean = getColumnValue[Boolean](columnIndex)

  override def getBoolean(columnLabel: String): Boolean = getBoolean(findColumn(columnLabel))

  override def getByte(columnIndex: Int): Byte = getColumnValue[Byte](columnIndex)

  override def getByte(columnLabel: String): Byte = getByte(findColumn(columnLabel))

  override def getShort(columnIndex: Int): Short = getColumnValue[Short](columnIndex)

  override def getShort(columnLabel: String): Short = getShort(findColumn(columnLabel))

  override def getInt(columnIndex: Int): Int = getColumnValue[Int](columnIndex)

  override def getInt(columnLabel: String): Int = getInt(findColumn(columnLabel))

  override def getLong(columnIndex: Int): Long = getColumnValue[Long](columnIndex)

  override def getLong(columnLabel: String): Long = getLong(findColumn(columnLabel))

  override def getFloat(columnIndex: Int): Float = getColumnValue[Float](columnIndex)

  override def getFloat(columnLabel: String): Float = getFloat(findColumn(columnLabel))

  override def getDouble(columnIndex: Int): Double = getColumnValue[Double](columnIndex)

  override def getDouble(columnLabel: String): Double = getDouble(findColumn(columnLabel))

  @deprecated
  override def getBigDecimal(columnIndex: Int, scale: Int): BigDecimal = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  @deprecated
  override def getBigDecimal(columnLabel: String, scale: Int): BigDecimal = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getBigDecimal(columnIndex: Int): BigDecimal = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getBigDecimal(columnLabel: String): BigDecimal = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getBytes(columnIndex: Int): scala.Array[Byte] = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getBytes(columnLabel: String): scala.Array[Byte] = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getDate(columnIndex: Int): Date = getColumnValue[Date](columnIndex)

  override def getDate(columnLabel: String): Date = getDate(findColumn(columnLabel))

  override def getDate(columnIndex: Int, cal: Calendar): Date = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getDate(columnLabel: String, cal: Calendar): Date = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getTime(columnIndex: Int): Time = getColumnValue[Time](columnIndex)

  override def getTime(columnLabel: String): Time = getTime(findColumn(columnLabel))

  override def getTime(columnIndex: Int, cal: Calendar): Time = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getTime(columnLabel: String, cal: Calendar): Time = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getTimestamp(columnIndex: Int): Timestamp = getColumnValue[Timestamp](columnIndex)

  override def getTimestamp(columnLabel: String): Timestamp = getTimestamp(findColumn(columnLabel))

  override def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getAsciiStream(columnIndex: Int): InputStream = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getAsciiStream(columnLabel: String): InputStream = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  @deprecated
  override def getUnicodeStream(columnIndex: Int): InputStream = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  @deprecated
  override def getUnicodeStream(columnLabel: String): InputStream = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getBinaryStream(columnIndex: Int): InputStream = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getBinaryStream(columnLabel: String): InputStream = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getWarnings(): SQLWarning = warningChain

  override def clearWarnings(): Unit = warningChain = null

  override def getCursorName(): String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMetaData(): ResultSetMetaData = new BitlapResultSetMetaData(columnNames, columnTypes)

  override def getObject(columnIndex: Int): Any = getColumnValue[Any](columnIndex)

  override def getObject(columnLabel: String): Any = getObject(findColumn(columnLabel))

  override def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getObject[T](columnIndex: Int, `type`: Class[T]): T = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getObject[T](columnLabel: String, `type`: Class[T]): T = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def findColumn(columnLabel: String): Int = {
    val columnIndex = columnNames.indexOf(columnLabel)
    if (columnIndex == -1) {
      throw BitlapSQLException("Bitlap SQL Exception")
    }
    columnIndex + 1
  }

  override def getCharacterStream(columnIndex: Int): Reader = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getCharacterStream(columnLabel: String): Reader = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def isBeforeFirst(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isAfterLast(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isFirst(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isLast(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def beforeFirst(): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def afterLast(): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def first(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def last(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getRow(): Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def absolute(row: Int): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def relative(rows: Int): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def previous(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setFetchDirection(direction: Int): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getFetchDirection(): Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setFetchSize(rows: Int): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getFetchSize(): Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getType(): Int = ResultSet.TYPE_FORWARD_ONLY

  override def getConcurrency(): Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def rowUpdated(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def rowInserted(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def rowDeleted(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateNull(columnIndex: Int): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateNull(columnLabel: String): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateBoolean(columnIndex: Int, x: Boolean): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateBoolean(columnLabel: String, x: Boolean): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateByte(columnIndex: Int, x: Byte): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateByte(columnLabel: String, x: Byte): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateShort(columnIndex: Int, x: Short): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateShort(columnLabel: String, x: Short): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateInt(columnIndex: Int, x: Int): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateInt(columnLabel: String, x: Int): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateLong(columnIndex: Int, x: Long): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateLong(columnLabel: String, x: Long): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateFloat(columnIndex: Int, x: Float): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateFloat(columnLabel: String, x: Float): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateDouble(columnIndex: Int, x: Double): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateDouble(columnLabel: String, x: Double): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateBigDecimal(columnIndex: Int, x: BigDecimal): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateBigDecimal(columnLabel: String, x: BigDecimal): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateString(columnIndex: Int, x: String): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateString(columnLabel: String, x: String): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateBytes(columnIndex: Int, x: scala.Array[Byte]): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateBytes(columnLabel: String, x: scala.Array[Byte]): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateDate(columnIndex: Int, x: Date): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateDate(columnLabel: String, x: Date): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateTime(columnIndex: Int, x: Time): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateTime(columnLabel: String, x: Time): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateTimestamp(columnIndex: Int, x: Timestamp): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateTimestamp(columnLabel: String, x: Timestamp): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateAsciiStream(columnIndex: Int, x: InputStream): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateAsciiStream(columnLabel: String, x: InputStream): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateBinaryStream(columnIndex: Int, x: InputStream): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateBinaryStream(columnLabel: String, x: InputStream): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateCharacterStream(columnIndex: Int, x: Reader): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateCharacterStream(columnLabel: String, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateObject(columnIndex: Int, x: Any, scaleOrLength: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateObject(columnIndex: Int, x: Any): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateObject(columnLabel: String, x: Any, scaleOrLength: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateObject(columnLabel: String, x: Any): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def insertRow(): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateRow(): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def deleteRow(): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def refreshRow(): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def cancelRowUpdates(): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def moveToInsertRow(): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def moveToCurrentRow(): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getStatement(): Statement = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getRef(columnIndex: Int): Ref = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getRef(columnLabel: String): Ref = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getBlob(columnIndex: Int): Blob = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getBlob(columnLabel: String): Blob = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getClob(columnIndex: Int): Clob = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getClob(columnLabel: String): Clob = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getArray(columnIndex: Int): java.sql.Array = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getArray(columnLabel: String): java.sql.Array = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getURL(columnIndex: Int): URL = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getURL(columnLabel: String): URL = throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateRef(columnIndex: Int, x: Ref): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateRef(columnLabel: String, x: Ref): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateBlob(columnIndex: Int, x: Blob): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateBlob(columnLabel: String, x: Blob): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateBlob(columnIndex: Int, inputStream: InputStream): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateBlob(columnLabel: String, inputStream: InputStream): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateClob(columnIndex: Int, x: Clob): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateClob(columnLabel: String, x: Clob): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateClob(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateClob(columnIndex: Int, reader: Reader): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateClob(columnLabel: String, reader: Reader): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateArray(columnIndex: Int, x: java.sql.Array): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateArray(columnLabel: String, x: java.sql.Array): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getRowId(columnIndex: Int): RowId = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getRowId(columnLabel: String): RowId = throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateRowId(columnIndex: Int, x: RowId): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateRowId(columnLabel: String, x: RowId): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getHoldability(): Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isClosed(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateNString(columnIndex: Int, nString: String): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateNString(columnLabel: String, nString: String): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateNClob(columnIndex: Int, nClob: NClob): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateNClob(columnLabel: String, nClob: NClob): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateNClob(columnIndex: Int, reader: Reader): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateNClob(columnLabel: String, reader: Reader): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getNClob(columnIndex: Int): NClob = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getNClob(columnLabel: String): NClob = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getSQLXML(columnIndex: Int): SQLXML = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getSQLXML(columnLabel: String): SQLXML = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getNString(columnIndex: Int): String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getNString(columnLabel: String): String = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getNCharacterStream(columnIndex: Int): Reader = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getNCharacterStream(columnLabel: String): Reader = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def updateNCharacterStream(columnIndex: Int, x: Reader): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updateNCharacterStream(columnLabel: String, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

//  @inline
  @implicitNotFound("Could not find an implicit ClassTag[\\${T}]")
  private def getColumnValue[T: ClassTag](columnIndex: Int): T = {
    if (row == null) {
      throw BitlapSQLException("No row found.")
    }
    val colVals = row.values
    if (colVals == null) throw BitlapSQLException("RowSet does not contain any columns!")
    if (columnIndex > colVals.size) {
      throw BitlapSQLException(s"Invalid columnIndex: $columnIndex")
    }

    val bColumnValue = colVals(columnIndex - 1)
    try {
      if (bColumnValue.isEmpty) {
        _wasNull = true
      }

      val columnType = getSchema.columns(columnIndex - 1).typeDesc
      deserialize[T](columnType, bColumnValue)
    } catch {
      case e: Exception => throw BitlapSQLException(msg = e.getLocalizedMessage, cause = e.getCause)
    }
  }

  def setSchema(schema: TableSchema): Unit = this.schema = schema

  def getSchema: TableSchema = this.schema
}
