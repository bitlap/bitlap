/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc.client

import org.bitlap.network.driver.proto._
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }

/**
 * This class mainly wraps the RPC call procedure used inside JDBC.
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
private[jdbc] class BitlapClient(uri: String, port: Int, props: Map[String, String]) {

  private val rpcClient = new BitlapSyncClient(uri, port, props)

  def openSession(): BSessionHandle =
    rpcClient
      .openSession(username = "", password = "", configuration = Map.empty)
      .toBSessionHandle()
  // TODO: Add heartbeat

  def closeSession(sessionHandle: BSessionHandle): Unit =
    rpcClient.closeSession(new SessionHandle(sessionHandle))

  def executeStatement(sessionHandle: BSessionHandle, statement: String): BOperationHandle =
    rpcClient
      .executeStatement(
        statement = statement,
        sessionHandle = new SessionHandle(sessionHandle)
      )
      .toBOperationHandle()

  def fetchResults(operationHandle: BOperationHandle): BRowSet =
    rpcClient.fetchResults(new OperationHandle(operationHandle)).toBRowSet

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

  def getResultSetMetadata(operationHandle: BOperationHandle): BTableSchema = ???
}
