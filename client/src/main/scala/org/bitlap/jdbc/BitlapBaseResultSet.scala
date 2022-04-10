/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.network.proto.driver.{ BRow, BTableSchema, BTypeId }

import java.io.{InputStream, Reader}
import java.math.BigDecimal
import java.net.URL
import java.sql.Blob
import java.sql.Clob
import java.sql.Date
import java.sql.NClob
import java.sql.Ref
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.RowId
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Statement
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.util
import java.util.Calendar

/**
 *
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

    override def close() {
        ???
    }

    override def wasNull(): Boolean = {
        return _wasNull
    }

    override def getString(columnIndex: Int): String = {
        return getColumnValue(columnIndex)
    }

    override def getString(columnLabel: String): String = {
        return getString(findColumn(columnLabel))
    }

    override def getBoolean(columnIndex: Int): Boolean = {
        return getColumnValue(columnIndex)
    }

    override def getBoolean(columnLabel: String): Boolean = {
        return getBoolean(findColumn(columnLabel))
    }

    override def getByte(columnIndex: Int): Byte = {
        ???
    }

    override def getByte(columnLabel: String): Byte = {
        ???
    }

    override def getShort(columnIndex: Int): Short = {
        return getColumnValue(columnIndex)
    }

    override def getShort(columnLabel: String): Short = {
        return getShort(findColumn(columnLabel))
    }

    override def getInt(columnIndex: Int): Int = {
        return getColumnValue(columnIndex)
    }

    override def getInt(columnLabel: String): Int = {
        return getInt(findColumn(columnLabel))
    }

    override def getLong(columnIndex: Int): Long = {
        return getColumnValue(columnIndex)
    }

    override def getLong(columnLabel: String): Long = {
        return getLong(findColumn(columnLabel))
    }

    override def getFloat(columnIndex: Int): Float = {
        ???
    }

    override def getFloat(columnLabel: String): Float = {
        ???
    }

    override def getDouble(columnIndex: Int): Double = {
        return getColumnValue(columnIndex)
    }

    override def getDouble(columnLabel: String): Double = {
        return getDouble(findColumn(columnLabel))
    }

    override def getBigDecimal(columnIndex: Int, scale: Int): BigDecimal = {
        ???
    }

    override def getBigDecimal(columnLabel: String, scale: Int): BigDecimal = {
        ???
    }

    override def getBigDecimal(columnIndex: Int): BigDecimal = {
        ???
    }

    override def getBigDecimal(columnLabel: String): BigDecimal = {
        ???
    }


    override def getBytes(columnIndex: Int): Array[Byte] = ???

    override def getBytes(columnLabel: String): Array[Byte] = {
        ???
    }

    override def getDate(columnIndex: Int): Date = {
        ???
    }

    override def getDate(columnLabel: String): Date = {
        ???
    }

    override def getDate(columnIndex: Int, cal: Calendar): Date = {
        ???
    }

    override def getDate(columnLabel: String, cal: Calendar): Date = {
        ???
    }

    override def getTime(columnIndex: Int): Time = {
        ???
    }

    override def getTime(columnLabel: String): Time = {
        ???
    }

    override def getTime(columnIndex: Int, cal: Calendar): Time = {
        ???
    }

    override def getTime(columnLabel: String, cal: Calendar): Time = {
        ???
    }

    override def getTimestamp(columnIndex: Int): Timestamp = {
        return getColumnValue(columnIndex)
    }

    override def getTimestamp(columnLabel: String): Timestamp = {
        return getTimestamp(findColumn(columnLabel))
    }

    override def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = {
        ???
    }

    override def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = {
        ???
    }

    override def getAsciiStream(columnIndex: Int): InputStream = {
        ???
    }

    override def getAsciiStream(columnLabel: String): InputStream = {
        ???
    }

    override def getUnicodeStream(columnIndex: Int): InputStream = {
        ???
    }

    override def getUnicodeStream(columnLabel: String): InputStream = {
        ???
    }

    override def getBinaryStream(columnIndex: Int): InputStream = {
        ???
    }

    override def getBinaryStream(columnLabel: String): InputStream = {
        ???
    }

    override def getWarnings(): SQLWarning = {
        return warningChain
    }

    override def clearWarnings() = {
        warningChain = null
    }

    override def getCursorName(): String = {
        ???
    }

    override def getMetaData(): ResultSetMetaData = {
        return new BitlapResultSetMetaData(columnNames.toList, columnTypes.toList)
    }

    override def getObject(columnIndex: Int): Any = {
        ???
    }

    override def getObject(columnLabel: String): Any = {
        ???
    }

    override def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef = ???

    override def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef = ???

    override def getObject[T](columnIndex: Int, `type`: Class[T]): T = ???

    override def getObject[T](columnLabel: String, `type`: Class[T]): T = ???

    override def findColumn(columnLabel: String): Int = {
        var columnIndex = columnNames.indexOf(columnLabel)
        if (columnIndex == -1) {
            throw BSQLException("Bitlap SQL Exception")
        } else {
            columnIndex = columnIndex + 1
        }
        columnIndex
    }

    override def getCharacterStream(columnIndex: Int): Reader = {
        ???
    }

    override def getCharacterStream(columnLabel: String): Reader = {
        ???
    }

    override def isBeforeFirst(): Boolean = {
        ???
    }

    override def isAfterLast(): Boolean = {
        ???
    }

    override def isFirst(): Boolean = {
        ???
    }

    override def isLast(): Boolean = {
        ???
    }

    override def beforeFirst() = {
        ???
    }

    override def afterLast() = {
        ???
    }

    override def first(): Boolean = {
        ???
    }

    override def last(): Boolean = {
        ???
    }

    override def getRow(): Int = {
        ???
    }

    override def absolute(row: Int): Boolean = {
        ???
    }

    override def relative(rows: Int): Boolean = {
        ???
    }

    override def previous(): Boolean = {
        ???
    }

    override def setFetchDirection(direction: Int) = {
        ???
    }

    override def getFetchDirection(): Int = {
        ???
    }

    override def setFetchSize(rows: Int): Unit = ???

    override def getFetchSize(): Int = {
        ???
    }

    override def getType(): Int = {
        return ResultSet.TYPE_FORWARD_ONLY
    }

    override def getConcurrency(): Int = {
        ???
    }

    override def rowUpdated(): Boolean = {
        ???
    }

    override def rowInserted(): Boolean = {
        ???
    }

    override def rowDeleted(): Boolean = {
        ???
    }

    override def updateNull(columnIndex: Int) = {
        ???
    }

    override def updateNull(columnLabel: String) = {
        ???
    }

    override def updateBoolean(columnIndex: Int, x: Boolean) = {
        ???
    }

    override def updateBoolean(columnLabel: String, x: Boolean) = {
        ???
    }

    override def updateByte(columnIndex: Int, x: Byte) = {
        ???
    }

    override def updateByte(columnLabel: String, x: Byte) = {
        ???
    }

    override def updateShort(columnIndex: Int, x: Short) = {
        ???
    }

    override def updateShort(columnLabel: String, x: Short) = {
        ???
    }

    override def updateInt(columnIndex: Int, x: Int) = {
        ???
    }

    override def updateInt(columnLabel: String, x: Int) = {
        ???
    }

    override def updateLong(columnIndex: Int, x: Long) = {
        ???
    }

    override def updateLong(columnLabel: String, x: Long) = {
        ???
    }

    override def updateFloat(columnIndex: Int, x: Float) = {
        ???
    }

    override def updateFloat(columnLabel: String, x: Float) = {
        ???
    }

    override def updateDouble(columnIndex: Int, x: Double) = {
        ???
    }

    override def updateDouble(columnLabel: String, x: Double) = {
        ???
    }

    override def updateBigDecimal(columnIndex: Int, x: BigDecimal) = {
        ???
    }

    override def updateBigDecimal(columnLabel: String, x: BigDecimal) = {
        ???
    }

    override def updateString(columnIndex: Int, x: String) = {
        ???
    }

    override def updateString(columnLabel: String, x: String) = {
        ???
    }

    override def updateBytes(columnIndex: Int, x: Array[Byte]): Unit = ???

    override def updateBytes(columnLabel: String, x: Array[Byte]): Unit = ???

    override def updateDate(columnIndex: Int, x: Date) = {
        ???
    }

    override def updateDate(columnLabel: String, x: Date) = {
        ???
    }

    override def updateTime(columnIndex: Int, x: Time) {
        ???
    }

    override def updateTime(columnLabel: String, x: Time) = {
        ???
    }

    override def updateTimestamp(columnIndex: Int, x: Timestamp) = {
        ???
    }

    override def updateTimestamp(columnLabel: String, x: Timestamp) = {
        ???
    }

    override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int) = {
        ???
    }

    override def updateAsciiStream(columnLabel: String, x: InputStream, length: Int) = {
        ???
    }

    override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long) = {
        ???
    }

    override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long) = {
        ???
    }

    override def updateAsciiStream(columnIndex: Int, x: InputStream) = {
        ???
    }

    override def updateAsciiStream(columnLabel: String, x: InputStream) = {
        ???
    }

    override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int) = {
        ???
    }

    override def updateBinaryStream(columnLabel: String, x: InputStream, length: Int) = {
        ???
    }

    override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long) = {
        ???
    }

    override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long) = {
        ???
    }

    override def updateBinaryStream(columnIndex: Int, x: InputStream) = {
        ???
    }

    override def updateBinaryStream(columnLabel: String, x: InputStream) = {
        ???
    }

    override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int) = {
        ???
    }

    override def updateCharacterStream(columnLabel: String, reader: Reader, length: Int) = {
        ???
    }

    override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long) = {
        ???
    }

    override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long) = {
        ???
    }

    override def updateCharacterStream(columnIndex: Int, x: Reader) = {
        ???
    }

    override def updateCharacterStream(columnLabel: String, reader: Reader) = {
        ???
    }

    override def updateObject(columnIndex: Int, x: Any, scaleOrLength: Int) = {
        ???
    }

    override def updateObject(columnIndex: Int, x: Any) = {
        ???
    }

    override def updateObject(columnLabel: String, x: Any, scaleOrLength: Int) = {
        ???
    }

    override def updateObject(columnLabel: String, x: Any) = {
        ???
    }

    override def insertRow() = {
        ???
    }

    override def updateRow() = {
        ???
    }

    override def deleteRow() = {
        ???
    }

    override def refreshRow() = {
        ???
    }

    override def cancelRowUpdates() = {
        ???
    }

    override def moveToInsertRow() = {
        ???
    }

    override def moveToCurrentRow() = {
        ???
    }

    override def getStatement(): Statement = {
        ???
    }

    override def getRef(columnIndex: Int): Ref = {
        ???
    }

    override def getRef(columnLabel: String): Ref = {
        ???
    }

    override def getBlob(columnIndex: Int): Blob = {
        ???
    }

    override def getBlob(columnLabel: String): Blob = {
        ???
    }

    override def getClob(columnIndex: Int): Clob = {
        ???
    }

    override def getClob(columnLabel: String): Clob = {
        ???
    }

    override def getArray(columnIndex: Int): java.sql.Array = {
        ???
    }

    override def getArray(columnLabel: String): java.sql.Array = {
        ???
    }

    override def getURL(columnIndex: Int): URL = {
        ???
    }

    override def getURL(columnLabel: String): URL = {
        ???
    }

    override def updateRef(columnIndex: Int, x: Ref) = {
        ???
    }

    override def updateRef(columnLabel: String, x: Ref) = {
        ???
    }

    override def updateBlob(columnIndex: Int, x: Blob) = {
        ???
    }

    override def updateBlob(columnLabel: String, x: Blob) = {
        ???
    }

    override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long) = {
        ???
    }

    override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long) = {
        ???
    }

    override def updateBlob(columnIndex: Int, inputStream: InputStream) = {
        ???
    }

    override def updateBlob(columnLabel: String, inputStream: InputStream) = {
        ???
    }

    override def updateClob(columnIndex: Int, x: Clob) = {
        ???
    }

    override def updateClob(columnLabel: String, x: Clob) = {
        ???
    }

    override def updateClob(columnIndex: Int, reader: Reader, length: Long) = {
        ???
    }

    override def updateClob(columnLabel: String, reader: Reader, length: Long) = {
        ???
    }

    override def updateClob(columnIndex: Int, reader: Reader) = {
        ???
    }

    override def updateClob(columnLabel: String, reader: Reader) = {
        ???
    }

    override def updateArray(columnIndex: Int, x: java.sql.Array) = {
        ???
    }

    override def updateArray(columnLabel: String, x: java.sql.Array) = {
        ???
    }

    override def getRowId(columnIndex: Int): RowId = {
        ???
    }

    override def getRowId(columnLabel: String): RowId = {
        ???
    }

    override def updateRowId(columnIndex: Int, x: RowId) = {
        ???
    }

    override def updateRowId(columnLabel: String, x: RowId) = {
        ???
    }

    override def getHoldability(): Int = {
        ???
    }

    override def isClosed(): Boolean = {
        ???
    }

    override def updateNString(columnIndex: Int, nString: String) = {
        ???
    }

    override def updateNString(columnLabel: String, nString: String) = {
        ???
    }

    override def updateNClob(columnIndex: Int, nClob: NClob) = {
        ???
    }

    override def updateNClob(columnLabel: String, nClob: NClob) = {
        ???
    }

    override def updateNClob(columnIndex: Int, reader: Reader, length: Long) = {
        ???
    }

    override def updateNClob(columnLabel: String, reader: Reader, length: Long) = {
        ???
    }

    override def updateNClob(columnIndex: Int, reader: Reader) = {
        ???
    }

    override def updateNClob(columnLabel: String, reader: Reader) = {
        ???
    }

    override def getNClob(columnIndex: Int): NClob = {
        ???
    }

    override def getNClob(columnLabel: String): NClob = {
        ???
    }

    override def getSQLXML(columnIndex: Int): SQLXML = {
        ???
    }

    override def getSQLXML(columnLabel: String): SQLXML = {
        ???
    }

    override def updateSQLXML(columnIndex: Int, xmlObject: SQLXML) = {
        ???
    }

    override def updateSQLXML(columnLabel: String, xmlObject: SQLXML) = {
        ???
    }

    override def getNString(columnIndex: Int): String = {
        ???
    }

    override def getNString(columnLabel: String): String = {
        ???
    }

    override def getNCharacterStream(columnIndex: Int): Reader = {
        ???
    }

    override def getNCharacterStream(columnLabel: String): Reader = {
        ???
    }

    override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long) = {
        ???
    }

    override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long) = {
        ???
    }

    override def updateNCharacterStream(columnIndex: Int, x: Reader) = {
        ???
    }

    override def updateNCharacterStream(columnLabel: String, reader: Reader) = ???

    @inline
    private def getColumnValue[T](columnIndex: Int): T = {
        if (row == null) {
            throw BSQLException("No row found.")
        }
        val colVals = row.getColValsList
        if (colVals == null) throw BSQLException("RowSet does not contain any columns!")
        if (columnIndex > colVals.size) {
            throw BSQLException("Invalid columnIndex: $columnIndex")
        }

    // In kotlin, We can not use e.g. Int?,Long?,Double? to override the java interface here.
        val bColumnValue = colVals.get(columnIndex - 1)
        try {
            if (bColumnValue.isEmpty) {
                _wasNull = true
            }

            val valueStr = bColumnValue.toStringUtf8
            val columnType = getSchema().getColumns(columnIndex - 1).getTypeDesc
            return (
              columnType match {
                  case BTypeId.B_TYPE_ID_STRING_TYPE =>
                      if (valueStr.isEmpty) "" else valueStr
                  case BTypeId.B_TYPE_ID_INT_TYPE =>
                        if (valueStr.nonEmpty) Integer.parseInt(valueStr) else 0
                  case BTypeId.B_TYPE_ID_DOUBLE_TYPE =>
                        if (valueStr.nonEmpty) java.lang.Double.parseDouble(valueStr) else 0.0
                  case BTypeId.B_TYPE_ID_SHORT_TYPE =>
                        if (valueStr.nonEmpty) java.lang.Short.parseShort(valueStr) else 0
                  case BTypeId.B_TYPE_ID_LONG_TYPE =>
                        if (valueStr.nonEmpty) java.lang.Long.parseLong(valueStr) else 0
                  case BTypeId.B_TYPE_ID_BOOLEAN_TYPE =>
                        if (valueStr.nonEmpty) java.lang.Boolean.valueOf(valueStr) else false
                  case BTypeId.B_TYPE_ID_TIMESTAMP_TYPE =>
                        if (valueStr.nonEmpty) Timestamp.from(Instant.ofEpochMilli(java.lang.Long.parseLong(valueStr))) else Timestamp.from(Instant.now())
                  case _ => throw BSQLException("Unrecognized column type:$columnType")
                }
              ).asInstanceOf[T]
        } catch {
            case e: Exception => throw e
        }
    }

    def setSchema(schema: BTableSchema) = {
        this.schema = schema
    }

    def getSchema(): BTableSchema = {
        return this.schema
    }
}
