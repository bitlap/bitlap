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
package org.bitlap.jdbc

import java.{ util, * }
import java.sql.*
import java.util.{ List as _, Properties }
import java.util.concurrent.Executor

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.*
import scala.util.control.Breaks.*

import org.bitlap.common.LiteralSQL._
import org.bitlap.common.exception.BitlapExceptions
import org.bitlap.common.exception.BitlapSQLException
import org.bitlap.network.{ Connection as _, _ }
import org.bitlap.network.handles.*
import org.bitlap.network.protocol.impl.Sync

import bitlap.rolls.core.jdbc.{ columns, sqlQ, ResultSetX, TypeRow1 }

/** Bitlap connection
 */
class BitlapConnection(uri: String, info: Properties) extends Connection {
  import Constants.*

  given Connection = this

  private var session: SessionHandle               = _
  private var closed                               = true
  private var readOnly                             = false
  private var warningChain: SQLWarning             = _
  private var client: Sync                         = _
  private var initFile: String                     = _
  private var maxRetries                           = BITLAP_DEFAULT_RETRIES
  private var connParams: JdbcConnectionParams     = _
  private var bitlapConfs: ListMap[String, String] = ListMap[String, String]()
  private var sessionVars: ListMap[String, String] = ListMap[String, String]()

  {

    connParams = Utils.parseUri(uri)
    sessionVars = connParams.sessionVars
    bitlapConfs = connParams.bitlapConfs
    sessionVars = sessionVars ++ ListMap(Constants.DBNAME_PROPERTY_KEY -> connParams.dbName)

    for kv <- info.entrySet.asScala do
      kv.getKey match {
        case key: String =>
          if key.startsWith(BITLAP_CONF_PREFIX) then
            bitlapConfs = bitlapConfs ++ ListMap(key.substring(BITLAP_CONF_PREFIX.length) -> info.getProperty(key))
        case _ =>
      }

    if info.containsKey(JdbcConnectionParams.AUTH_USER) then {
      sessionVars =
        sessionVars ++ ListMap(JdbcConnectionParams.AUTH_USER -> info.getProperty(JdbcConnectionParams.AUTH_USER))
      if info.containsKey(JdbcConnectionParams.AUTH_PASSWD) then
        sessionVars =
          sessionVars ++ ListMap(JdbcConnectionParams.AUTH_PASSWD -> info.getProperty(JdbcConnectionParams.AUTH_PASSWD))
    }
    if info.containsKey(JdbcConnectionParams.AUTH_TYPE) then
      sessionVars =
        sessionVars ++ ListMap(JdbcConnectionParams.AUTH_TYPE -> info.getProperty(JdbcConnectionParams.AUTH_TYPE))

    maxRetries = bitlapConfs
      .get(BITLAP_RETRIES)
      .map(_.toIntOption)
      .map(_.getOrElse(BITLAP_DEFAULT_RETRIES))
      .getOrElse(BITLAP_DEFAULT_RETRIES)
    initFile = bitlapConfs.get(BITLAP_INIT_SQL).orNull

    var numRetries    = 0
    val retryInterval = 1000L
    breakable {
      while numRetries < maxRetries do
        try {
          client = new Sync(connParams.authorityList.toList.map(_.asServerAddress), bitlapConfs ++ sessionVars)
          session = client.openSession(
            sessionVars.getOrElse(JdbcConnectionParams.AUTH_USER, "root"),
            sessionVars.getOrElse(JdbcConnectionParams.AUTH_PASSWD, ""),
            sessionVars
          )
          executeInitSql()
          closed = false
          break
        } catch {
          case e: Exception =>
            numRetries += 1
            val errMsg: String = null
            val warnMsg        = s"Could not open client transport."
            try close()
            catch {
              case _: Exception =>
            }
            if numRetries >= maxRetries then throw BitlapSQLException(s"${e.getMessage}", cause = Option(e))
            else {
              System.err.println(
                s"$warnMsg${e.getMessage} Retrying $numRetries of $maxRetries with retry interval $retryInterval ms"
              )
              try Thread.sleep(retryInterval)
              catch {
                case _: InterruptedException =>
              }
            }
        }
    }
  }

  private def checkConnection(action: String): Unit =
    if closed then throw BitlapSQLException(s"Cannot $action after connection has been closed")

  private def executeInitSql(): Unit =
    if initFile != null && session != null then
      try {
        val st = new BitlapStatement(this, session, client)
        try {
          val sqlList = Utils.parseInitFile(initFile)
          for sql <- sqlList do {
            val hasResult = st.execute(sql)
            if hasResult then
              try {
                val rs = st.getResultSet()
                try while rs.next do println(rs.getString(1))
                catch { case ignore: Exception => }
                finally if rs != null then rs.close()
              } catch { case ignore: Exception => }
          }
        } catch {
          case e: Exception =>
            throw BitlapSQLException(e.getMessage)
        } finally if st != null then st.close()
      }

  override def unwrap[T](iface: Class[T]): T = {
    if (!iface.isInstance(this)) {
      throw BitlapExceptions.illegalException(iface.getName)
    }
    this.asInstanceOf[T]
  }

  override def isWrapperFor(iface: Class[?]): Boolean = iface.isInstance(this)

  override def close(): Unit =
    try
      if session != null then {
        client.closeSession(session)
      }
    finally closed = true

  override def createStatement(): Statement = {
    checkConnection("createStatement")
    new BitlapStatement(this, session, client)
  }

  override def isClosed(): Boolean = closed

  override def clearWarnings(): Unit = warningChain = null

  override def prepareStatement(sql: String): PreparedStatement = {
    checkConnection("prepareStatement")
    new BitlapPreparedStatement(this, session, client, sql)
  }

  override def prepareCall(sql: String): CallableStatement =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def nativeSQL(sql: String): String =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setAutoCommit(autoCommit: Boolean): Unit = ()

  override def getAutoCommit: Boolean = false

  override def commit(): Unit = ()

  override def rollback(): Unit = ()

  override def getMetaData: DatabaseMetaData = {
    checkConnection("getMetaData")
    new BitlapDatabaseMetaData(this, session, client)
  }

  override def setReadOnly(readOnly: Boolean): Unit = this.readOnly = readOnly

  override def isReadOnly: Boolean = this.readOnly

  override def setCatalog(catalog: String): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getCatalog: String = ""

  override def setTransactionIsolation(level: Int): Unit = ()

  override def getTransactionIsolation: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getWarnings: SQLWarning = warningChain

  override def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement = {
    checkConnection("createStatement")
    new BitlapStatement(this, session, client)
  }

  override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement = {
    checkConnection("prepareStatement")
    new BitlapPreparedStatement(this, session, client, sql)
  }

  override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int): CallableStatement =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getTypeMap: util.Map[String, Class[?]] = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setTypeMap(map: util.Map[String, Class[?]]): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setHoldability(holdability: Int): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getHoldability: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setSavepoint(): Savepoint = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setSavepoint(name: String): Savepoint = throw new SQLFeatureNotSupportedException("Method not supported")

  override def rollback(savepoint: Savepoint): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def releaseSavepoint(savepoint: Savepoint): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement = {
    checkConnection("createStatement")
    new BitlapStatement(this, session, client)
  }

  override def prepareStatement(
    sql: String,
    resultSetType: Int,
    resultSetConcurrency: Int,
    resultSetHoldability: Int
  ): PreparedStatement = {
    checkConnection("prepareStatement")
    new BitlapPreparedStatement(this, session, client, sql)
  }

  override def prepareCall(
    sql: String,
    resultSetType: Int,
    resultSetConcurrency: Int,
    resultSetHoldability: Int
  ): CallableStatement = throw new SQLFeatureNotSupportedException("Method not supported")

  override def prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement = {
    checkConnection("prepareStatement")
    new BitlapPreparedStatement(this, session, client, sql)
  }

  override def prepareStatement(sql: String, columnIndexes: scala.Array[Int]): PreparedStatement = {
    checkConnection("prepareStatement")
    new BitlapPreparedStatement(this, session, client, sql)
  }

  override def prepareStatement(sql: String, columnNames: scala.Array[String]): PreparedStatement = {
    checkConnection("prepareStatement")
    new BitlapPreparedStatement(this, session, client, sql)
  }

  override def createClob(): Clob = throw new SQLFeatureNotSupportedException("Method not supported")

  override def createBlob(): Blob = throw new SQLFeatureNotSupportedException("Method not supported")

  override def createNClob(): NClob = throw new SQLFeatureNotSupportedException("Method not supported")

  override def createSQLXML(): SQLXML = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isValid(timeout: Int): Boolean = {
    if timeout < 0 then throw BitlapSQLException("timeout value was negative")
    if closed then return false
    var rc = false
    try {
      new BitlapDatabaseMetaData(this, session, client).getDatabaseProductName
      rc = true
    } catch {
      case _: SQLException =>
    }
    rc
  }

  override def setClientInfo(name: String, value: String): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setClientInfo(properties: Properties): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getClientInfo(name: String): String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getClientInfo: Properties = throw new SQLFeatureNotSupportedException("Method not supported")

  override def createArrayOf(typeName: String, elements: scala.Array[AnyRef]): sql.Array =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def createStruct(typeName: String, attributes: scala.Array[AnyRef]): Struct =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def setSchema(schema: String): Unit = {
    checkConnection("setSchema")
    if schema == null || schema.isEmpty then throw BitlapSQLException("Schema name is null or empty")
    ResultSetX[TypeRow1[String]](sqlQ"USE $schema").fetch()
  }

  override def getSchema: String = {
    checkConnection("getSchema")
    val rs   = ResultSetX[TypeRow1[String]](sqlQ"${ShowCurrentDatabase.command}")
    val data = rs.fetch()
    if (data.nonEmpty) {
      data.headOption.map(_.columns[rs.Out]._1).orNull
    } else {
      null
    }
  }

  override def abort(executor: Executor): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getNetworkTimeout: Int = throw new SQLFeatureNotSupportedException("Method not supported")
}
