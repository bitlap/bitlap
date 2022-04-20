/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.common.BitlapConf
import org.bitlap.network.RPC
import org.bitlap.network.driver.proto._

import java.lang.{Long => JLong}
import scala.jdk.CollectionConverters._

/**
 * This class mainly wraps the RPC call procedure used inside JDBC.
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
private[jdbc] class BitlapClient(uri: String, port: Int, props: Map[String, String]) {

  private lazy val conf: BitlapConf = new BitlapConf(props.asJava)
  private val rpcClient = RPC.newClient(uri, port)
  private val readTimeout: JLong = conf.get(BitlapConf.NODE_READ_TIMEOUT)

  /**
   * Used to open a session during JDBC connection initialization.
   */
  def openSession(): BSessionHandle =
    rpcClient
      .syncOpenSession(BOpenSession.BOpenSessionReq(username = "", password = "", configuration = Map.empty))
      .getSessionHandle
  // TODO: Add heartbeat

  /**
   * Used to close the session when the JDBC connection is closed.
   */
  def closeSession(sessionHandle: BSessionHandle): Unit =
    rpcClient.syncCloseSession(BCloseSession.BCloseSessionReq(Some(sessionHandle)))

  /**
   * Used to execute normal SQL by JDBC. Does not contain `?` placeholders.
   */
  def executeStatement(sessionHandle: BSessionHandle, statement: String): BOperationHandle =
    rpcClient
      .syncExecuteStatement(
        BExecuteStatement.BExecuteStatementReq(statement, Some(sessionHandle), Map.empty, readTimeout)
      )
      .getOperationHandle

  /**
   * Used for JDBC to get result set of the specified operation.
   */
  def fetchResults(operationHandle: BOperationHandle): BFetchResults.BFetchResultsResp = ???

  def getSchemas(
    sessionHandle: BSessionHandle,
    catalogName: String = null,
    schemaName: String = null
  ): BOperationHandle = ???

  def getTables(
    sessionHandle: BSessionHandle,
    tableName: String = null,
    schemaName: String = null
  ): BOperationHandle = ???

  def getColumns(
    sessionHandle: BSessionHandle,
    tableName: String = null,
    schemaName: String = null,
    columnName: String = null
  ): BOperationHandle = ???

  /**
   * Used for JDBC to get Schema of the specified operation.
   */
  def getResultSetMetadata(operationHandle: BOperationHandle): BTableSchema = ???
}
