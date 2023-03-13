/* Copyright (c) 2023 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.client.BitlapClient
import org.bitlap.network.handles._

import java.sql._
import java.util.concurrent.Executor
import java.util.{ List => _, Properties }
import java.{ util, _ }
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks._

/** bitlap Connection
 *
 *  @author
 *    梦境迷离
 *  @since 2021/6/6
 *  @version 1.0
 */
class BitlapConnection(uri: String, info: Properties) extends Connection {
  import Constants._

  private var session: SessionHandle               = _
  private var closed                               = true
  private var warningChain: SQLWarning             = _
  private var client: BitlapClient                 = _
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

    for (kv <- info.entrySet.asScala)
      kv.getKey match {
        case key: String =>
          if (key.startsWith(BITLAP_CONF_PREFIX))
            bitlapConfs = bitlapConfs ++ ListMap(key.substring(BITLAP_CONF_PREFIX.length) -> info.getProperty(key))
        case _ =>
      }

    if (info.containsKey(JdbcConnectionParams.AUTH_USER)) {
      sessionVars =
        sessionVars ++ ListMap(JdbcConnectionParams.AUTH_USER -> info.getProperty(JdbcConnectionParams.AUTH_USER))
      if (info.containsKey(JdbcConnectionParams.AUTH_PASSWD))
        sessionVars =
          sessionVars ++ ListMap(JdbcConnectionParams.AUTH_PASSWD -> info.getProperty(JdbcConnectionParams.AUTH_PASSWD))
    }
    if (info.containsKey(JdbcConnectionParams.AUTH_TYPE))
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
      while (numRetries < maxRetries)
        try {
          client = new BitlapClient(connParams.authorityList, bitlapConfs ++ sessionVars)
          session = client.openSession()
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
            if (numRetries >= maxRetries)
              throw BitlapSQLException(s"$errMsg${e.getMessage}", " 08S01", cause = e)
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
    if (closed) throw BitlapSQLException(s"Cannot $action after connection has been closed")

  private def executeInitSql(): Unit =
    if (initFile != null) try {
      val st = createStatement()
      try {
        val sqlList = Utils.parseInitFile(initFile)
        for (sql <- sqlList) {
          println(s"Executing InitSql ... $sql")
          val hasResult = st.execute(sql)
          if (hasResult) try {
            val rs = st.getResultSet
            try while (rs.next) println(rs.getString(1))
            catch { case ignore: Exception => }
            finally if (rs != null) rs.close()
          } catch { case ignore: Exception => }
        }
      } catch {
        case e: Exception =>
          throw BitlapSQLException(e.getMessage)
      } finally if (st != null) st.close()
    }

  override def unwrap[T](iface: Class[T]): T = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isWrapperFor(iface: Class[_]): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def close(): Unit =
    try
      if (session != null) {
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

  override def prepareCall(sql: String): CallableStatement = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def nativeSQL(sql: String): String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setAutoCommit(autoCommit: Boolean): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getAutoCommit: Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def commit(): Unit = ()

  override def rollback(): Unit = ()

  override def getMetaData: DatabaseMetaData = {
    checkConnection("getMetaData")
    new BitlapDatabaseMetaData(this, session, client)
  }

  override def setReadOnly(readOnly: Boolean): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isReadOnly: Boolean = false

  override def setCatalog(catalog: String): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getCatalog: String = ""

  override def setTransactionIsolation(level: Int): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

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

  override def getTypeMap: util.Map[String, Class[_]] = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def setTypeMap(map: util.Map[String, Class[_]]): Unit = throw new SQLFeatureNotSupportedException(
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
    if (timeout < 0) throw BitlapSQLException("timeout value was negative")
    if (closed) return false
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
    // JDK 1.7
    checkConnection("setSchema")
    if (schema == null || schema.isEmpty) throw BitlapSQLException("Schema name is null or empty")
    if (schema.contains(";")) throw BitlapSQLException("invalid schema name")
    var stmt: Statement = null
    try {
      stmt = createStatement()
      stmt.execute("USE " + schema)
    } finally if (stmt != null) stmt.close()
  }

  override def getSchema: String = {
    checkConnection("getSchema")
    var res: ResultSet  = null
    var stmt: Statement = null
    try {
      stmt = createStatement()
      res = stmt.executeQuery("SHOW CURRENT_DATABASE")
      if (res == null || !res.next) throw BitlapSQLException("Failed to get schema information")
    } finally {
      if (res != null) res.close()
      if (stmt != null) stmt.close()
    }
    res.getString(1)
  }

  override def abort(executor: Executor): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getNetworkTimeout: Int = throw new SQLFeatureNotSupportedException("Method not supported")
}
