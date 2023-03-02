/* Copyright (c) 2023 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.client.BitlapClient
import org.bitlap.network.handles.SessionHandle

import java.io.{ InputStream, Reader }
import java.math.BigDecimal
import java.net.URL
import java.sql._
import java.text.MessageFormat
import java.util.{ Calendar, Scanner }
import scala.collection.mutable
import scala.util.control.Breaks.{ break, breakable }
import scala.{ Array => _, Byte => _ }

/** @author
 *    梦境迷离
 *  @version 1.0,2023/2/25
 */
class BitlapPreparedStatement(
  private val connection: Connection,
  private val sessHandle: SessionHandle,
  private val client: BitlapClient,
  private val sql: String
) extends BitlapStatement(connection, sessHandle, client)
    with PreparedStatement {

  private val parameters = new mutable.HashMap[Int, String]

  private def getCharIndexFromSqlByParamLocation(sql: String, cchar: Char, paramLoc: Int): Int = {
    var signalCount = 0
    var charIndex   = -1
    var num         = 0
    breakable {
      for (i <- 0 until sql.length) {
        val c = sql.charAt(i)
        if (c == '\'' || c == '\\') // record the count of char "'" and char "\"
          signalCount += 1
        else if (c == cchar && signalCount % 2 == 0) { // check if the ? is really the parameter
          num += 1
          if (num == paramLoc) {
            charIndex = i
            break()
          }
        }
      }
    }
    charIndex
  }

  private def updateSql(sql: String, parameters: mutable.HashMap[Int, String]): String = {
    if (!sql.contains("?")) return sql
    val newSql   = new StringBuffer(sql)
    var paramLoc = 1
    while (getCharIndexFromSqlByParamLocation(sql, '?', paramLoc) > 0) {
      // check the user has set the needs parameters
      if (parameters.contains(paramLoc)) {
        val tt = getCharIndexFromSqlByParamLocation(newSql.toString, '?', 1)
        newSql.deleteCharAt(tt)
        newSql.insert(tt, parameters.get(paramLoc))
      }
      paramLoc += 1
    }
    newSql.toString
  }

  override def execute: Boolean = super.execute(updateSql(sql, parameters))

  override def executeQuery(): ResultSet = super.executeQuery(updateSql(sql, parameters))

  override def executeUpdate(): Int = {
    super.executeUpdate(updateSql(sql, parameters))
    0
  }

  override def setNull(parameterIndex: Int, sqlType: Int): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setBoolean(parameterIndex: Int, x: Boolean): Unit = parameters.put(parameterIndex, "" + x)

  override def setByte(parameterIndex: Int, x: Byte): Unit = parameters.put(parameterIndex, "" + x)

  override def setShort(parameterIndex: Int, x: Short): Unit = parameters.put(parameterIndex, "" + x)

  override def setInt(parameterIndex: Int, x: Int): Unit = parameters.put(parameterIndex, "" + x)

  override def setLong(parameterIndex: Int, x: Long): Unit = parameters.put(parameterIndex, "" + x)

  override def setFloat(parameterIndex: Int, x: Float): Unit = parameters.put(parameterIndex, "" + x)

  override def setDouble(parameterIndex: Int, x: Double): Unit = parameters.put(parameterIndex, "" + x)

  override def setBigDecimal(parameterIndex: Int, x: java.math.BigDecimal): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setString(parameterIndex: Int, x: String): Unit = parameters.put(parameterIndex, "'" + x + "'")

  override def setBytes(parameterIndex: Int, x: scala.Array[Byte]): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setDate(parameterIndex: Int, x: Date): Unit = parameters.put(parameterIndex, x.toString)

  override def setTime(parameterIndex: Int, x: Time): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setTimestamp(parameterIndex: Int, x: Timestamp): Unit = parameters.put(parameterIndex, x.toString)

  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setUnicodeStream(parameterIndex: Int, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def clearParameters(): Unit = this.parameters.clear()

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setObject(parameterIndex: Int, x: Any): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def addBatch(): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setRef(parameterIndex: Int, x: Ref): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setBlob(parameterIndex: Int, x: Blob): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setClob(parameterIndex: Int, x: Clob): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setArray(parameterIndex: Int, x: Array): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getMetaData: ResultSetMetaData = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setDate(parameterIndex: Int, x: Date, cal: Calendar): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setTime(parameterIndex: Int, x: Time, cal: Calendar): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setTimestamp(parameterIndex: Int, x: Timestamp, cal: Calendar): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setNull(parameterIndex: Int, sqlType: Int, typeName: String): Unit =
    parameters.put(parameterIndex, "NULL")

  override def setURL(parameterIndex: Int, x: URL): Unit = parameters.put(parameterIndex, "NULL")

  override def getParameterMetaData: ParameterMetaData = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setRowId(parameterIndex: Int, x: RowId): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setNString(parameterIndex: Int, value: String): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setNCharacterStream(parameterIndex: Int, value: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setNClob(parameterIndex: Int, value: NClob): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setClob(parameterIndex: Int, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setBlob(parameterIndex: Int, inputStream: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setNClob(parameterIndex: Int, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setSQLXML(parameterIndex: Int, xmlObject: SQLXML): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int, scaleOrLength: Int): Unit =
    if (x == null) setNull(parameterIndex, Types.NULL)
    else
      x match {
        case str: String         => setString(parameterIndex, str)
        case sh: Short           => setShort(parameterIndex, sh)
        case i: Int              => setInt(parameterIndex, i)
        case l: Long             => setLong(parameterIndex, l)
        case fl: Float           => setFloat(parameterIndex, fl)
        case d: Double           => setDouble(parameterIndex, d)
        case bool: Boolean       => setBoolean(parameterIndex, bool)
        case b: Byte             => setByte(parameterIndex, b)
        case _: Char             => setString(parameterIndex, x.toString)
        case _: scala.BigDecimal => setString(parameterIndex, x.toString)
        case _: Timestamp        => setString(parameterIndex, x.toString)
        // jva types
        case _: BigDecimal           => setString(parameterIndex, x.toString)
        case sh: java.lang.Short     => setShort(parameterIndex, sh.shortValue())
        case i: java.lang.Integer    => setInt(parameterIndex, i.intValue())
        case l: java.lang.Long       => setLong(parameterIndex, l.longValue())
        case fl: java.lang.Float     => setFloat(parameterIndex, fl.floatValue())
        case d: java.lang.Double     => setDouble(parameterIndex, d.doubleValue())
        case bool: java.lang.Boolean => setBoolean(parameterIndex, bool.booleanValue())
        case b: java.lang.Byte       => setByte(parameterIndex, b.byteValue())
        case _: java.lang.Character  => setString(parameterIndex, x.toString)
        case _                       =>
          // Can't infer a type.
          throw BitlapSQLException(
            MessageFormat.format(
              "Can''t infer the SQL type to use for an instance of {0}. Use setObject() with an explicit Types value to specify the type to use.",
              x.getClass.getName
            )
          )
      }

  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setAsciiStream(parameterIndex: Int, x: InputStream): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setBinaryStream(parameterIndex: Int, x: InputStream): Unit = {
    val str = new Scanner(x, "UTF-8").useDelimiter("\\A").next
    parameters.put(parameterIndex, str)
  }

  override def setCharacterStream(parameterIndex: Int, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setNCharacterStream(parameterIndex: Int, value: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setClob(parameterIndex: Int, reader: Reader): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setBlob(parameterIndex: Int, inputStream: InputStream): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setNClob(parameterIndex: Int, reader: Reader): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )
}
