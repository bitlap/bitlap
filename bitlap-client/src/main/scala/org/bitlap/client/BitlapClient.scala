/* Copyright (c) 2022 bitlap.org */
package org.bitlap.client

import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ RowSet, TableSchema }

/**
 * This class mainly wraps the RPC call procedure used inside JDBC.
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
private[bitlap] class BitlapClient(uri: String, port: Int, props: Map[String, String]) {

  private val rpcClient = new BitlapSyncClient(uri, port, props)

  def openSession(): SessionHandle =
    rpcClient
      .openSession(username = "", password = "", configuration = props)
  // TODO: Add heartbeat

  def closeSession(sessionHandle: SessionHandle): Unit =
    rpcClient.closeSession(sessionHandle)

  def executeStatement(sessionHandle: SessionHandle, statement: String): OperationHandle =
    rpcClient
      .executeStatement(
        statement = statement,
        sessionHandle = sessionHandle
      )

  def fetchResults(operationHandle: OperationHandle): RowSet =
    rpcClient.fetchResults(operationHandle).results

  // make it return schema data, instead of `BOperationHandle` ?
  def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String = null,
    schemaName: String = null
  ): OperationHandle = ???

  def getTables(
    sessionHandle: SessionHandle,
    tableName: String = null,
    schemaName: String = null
  ): OperationHandle = ???

  def getColumns(
    sessionHandle: SessionHandle,
    tableName: String = null,
    schemaName: String = null,
    columnName: String = null
  ): OperationHandle = ???

  def getResultSetMetadata(operationHandle: OperationHandle): TableSchema =
    rpcClient.getResultSetMetadata(operationHandle)
}
