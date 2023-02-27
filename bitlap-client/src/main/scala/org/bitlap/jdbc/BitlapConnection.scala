/* Copyright (c) 2023 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.client.BitlapClient
import org.bitlap.jdbc.BitlapConnection.URI_PREFIX
import org.bitlap.network.handles._
import org.bitlap.tools.apply

import java._
import java.io._
import java.sql._
import java.util.{ ArrayList => JArrayList, List => _, Properties }
import java.util.concurrent.Executor
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks._

/** bitlap Connection
 *
 *  @author
 *    梦境迷离
 *  @since 2021/6/6
 *  @version 1.0
 */
@apply
class BitlapConnection(uri: String, info: Properties) extends Connection {
  // apply not support default args

  private var session: SessionHandle   = _
  private var closed                   = true
  private var warningChain: SQLWarning = _
  private var client: BitlapClient     = _
  private var initFile: String         = _
  private var maxRetries               = 1

  {
    if (!uri.startsWith(URI_PREFIX)) {
      throw BitlapSQLException(s"Invalid URL: $uri")
    }
    // remove prefix
    val uriWithoutPrefix = uri.substring(URI_PREFIX.length)
    val hosts            = uriWithoutPrefix.split(",")
    // parse uri
    val serverPeers = hosts.map(f => f.split("/")(0))
    val db          = hosts.filter(_.contains("/")).map(_.split("/")(1)).headOption.getOrElse(Constants.DEFAULT_DB)
    info.put(Constants.DBNAME_PROPERTY_KEY, db)

    // TODO get args from url query parameters
    if (info.containsKey("initFile")) {
      initFile = info.getProperty("initFile")
    }

    if (info.containsKey("retries")) {
      maxRetries = info.getProperty("retries").toInt
    }
    var numRetries    = 0
    val retryInterval = 1000L
    breakable {
      while (numRetries < maxRetries)
        try {
          client = new BitlapClient(serverPeers, info.asScala.toMap)
          session = client.openSession()
          executeInitSql()
          closed = false
          break
        } catch {
          case e: Exception =>
            numRetries += 1
            val errMsg: String = null
            val warnMsg        = s"Could not open client transport with JDBC Uri: $uriWithoutPrefix: "
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
        val sqlList = parseInitFile(initFile)
        for (sql <- sqlList) {
          println(s"Executing InitSql ... $sql")
          val hasResult = st.execute(sql)
          if (hasResult) try {
            val rs = st.getResultSet
            try while (rs.next) println(rs.getString(1))
            finally if (rs != null) rs.close()
          }
        }
      } catch {
        case e: Exception =>
          throw BitlapSQLException(e.getMessage)
      } finally if (st != null) st.close()
    }

  // TODO functional style
  private def parseInitFile(initFile: String): List[String] = {
    val file                      = new File(initFile)
    var br: BufferedReader        = null
    var initSqlList: List[String] = Nil
    try {
      val input = new FileInputStream(file)
      br = new BufferedReader(new InputStreamReader(input, "UTF-8"))
      var line: String = null
      val sb           = new mutable.StringBuilder("")
      while ({
        line = br.readLine
        line != null
      }) {
        line = line.trim
        if (line.nonEmpty) {
          if (line.startsWith("#") || line.startsWith("--")) {
            // todo: continue is not supported
          } else {
            line = line.concat(" ")
            sb.append(line)
          }
        }
      }
      initSqlList = getInitSql(sb.toString)
    } catch {
      case e: IOException =>
        throw new IOException(e)
    } finally if (br != null) br.close()
    initSqlList
  }

  // TODO functional style
  private def getInitSql(sbLine: String): List[String] = {
    val sqlArray    = sbLine.toCharArray
    val initSqlList = new JArrayList[String]
    var index       = 0
    var beginIndex  = 0
    while (index < sqlArray.length) {
      if (sqlArray(index) == ';') {
        val sql = sbLine.substring(beginIndex, index).trim
        initSqlList.add(sql)
        beginIndex = index + 1
      }

      index += 1
    }
    initSqlList.asScala.toList
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
object BitlapConnection {
  private val URI_PREFIX = "jdbc:bitlap://"
}
