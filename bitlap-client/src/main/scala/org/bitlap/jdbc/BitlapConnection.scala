/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.client.BitlapClient
import org.bitlap.jdbc.BitlapConnection.URI_PREFIX
import org.bitlap.network.handles._
import org.bitlap.tools.apply

import java.sql._
import java.util.Properties
import java.util.concurrent.Executor
import java.{ sql, util }
import scala.jdk.CollectionConverters._

/** Bitlap Connection
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

  {
    if (!uri.startsWith(URI_PREFIX)) {
      throw BitlapSQLException(s"Invalid URL: $uri")
    }
    // remove prefix
    val uriWithoutPrefix = uri.substring(URI_PREFIX.length)
    val hosts            = uriWithoutPrefix.split(",")
    // parse uri
    val parts       = hosts.map(it => it.split("/"))
    val serverPeers = hosts.map(f => f.split("/")(0))
    try {
      // FIXME
      val hostAndPort =
        try parts.map(_(0)).mkString(",").split(":")
        catch {
          case e: Exception =>
            e.printStackTrace(); println("Use default port: 23333"); scala.Array("localhost", "23333")
        }
      client = new BitlapClient(serverPeers, info.asScala.toMap)
      session = client.openSession()
      closed = false
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw BitlapSQLException(
          s"Bitlap openSession failed, connect to serverPeers: ${serverPeers.mkString("Array(", ", ", ")")}",
          cause = e
        )
    }
  }

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???

  override def close(): Unit =
    try
      if (session != null) {
        client.closeSession(session)
      }
    finally closed = true

  override def createStatement(): Statement =
    if (session != null) {
      new BitlapStatement(session, client)
    } else {
      throw BitlapSQLException("Statement is closed")
    }

  override def isClosed(): Boolean = closed

  override def clearWarnings(): Unit = warningChain = null

  override def prepareStatement(sql: String): PreparedStatement = ???

  override def prepareCall(sql: String): CallableStatement = ???

  override def nativeSQL(sql: String): String = ???

  override def setAutoCommit(autoCommit: Boolean): Unit = ???

  override def getAutoCommit: Boolean = ???

  override def commit(): Unit = ()

  override def rollback(): Unit = ()

  override def getMetaData: DatabaseMetaData = ???

  override def setReadOnly(readOnly: Boolean): Unit = ???

  override def isReadOnly: Boolean = ???

  override def setCatalog(catalog: String): Unit = ???

  override def getCatalog: String = ???

  override def setTransactionIsolation(level: Int): Unit = ???

  override def getTransactionIsolation: Int = ???

  override def getWarnings: SQLWarning = warningChain

  override def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement = ???

  override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement = ???

  override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int): CallableStatement = ???

  override def getTypeMap: util.Map[String, Class[_]] = ???

  override def setTypeMap(map: util.Map[String, Class[_]]): Unit = ???

  override def setHoldability(holdability: Int): Unit = ???

  override def getHoldability: Int = ???

  override def setSavepoint(): Savepoint = ???

  override def setSavepoint(name: String): Savepoint = ???

  override def rollback(savepoint: Savepoint): Unit = ???

  override def releaseSavepoint(savepoint: Savepoint): Unit = ???

  override def createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement =
    ???

  override def prepareStatement(
    sql: String,
    resultSetType: Int,
    resultSetConcurrency: Int,
    resultSetHoldability: Int
  ): PreparedStatement = ???

  override def prepareCall(
    sql: String,
    resultSetType: Int,
    resultSetConcurrency: Int,
    resultSetHoldability: Int
  ): CallableStatement = ???

  override def prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement = ???

  override def prepareStatement(sql: String, columnIndexes: scala.Array[Int]): PreparedStatement = ???

  override def prepareStatement(sql: String, columnNames: scala.Array[String]): PreparedStatement = ???

  override def createClob(): Clob = ???

  override def createBlob(): Blob = ???

  override def createNClob(): NClob = ???

  override def createSQLXML(): SQLXML = ???

  override def isValid(timeout: Int): Boolean = ???

  override def setClientInfo(name: String, value: String): Unit = ???

  override def setClientInfo(properties: Properties): Unit = ???

  override def getClientInfo(name: String): String = ???

  override def getClientInfo: Properties = ???

  override def createArrayOf(typeName: String, elements: scala.Array[AnyRef]): sql.Array = ???

  override def createStruct(typeName: String, attributes: scala.Array[AnyRef]): Struct = ???

  override def setSchema(schema: String): Unit = ???

  override def getSchema: String = "default" // TODO: fix me

  override def abort(executor: Executor): Unit = ???

  override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit = ???

  override def getNetworkTimeout: Int = ???
}
object BitlapConnection {
  private val URI_PREFIX = "jdbc:bitlap://"
}
