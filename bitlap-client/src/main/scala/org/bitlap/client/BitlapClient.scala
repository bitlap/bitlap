/* Copyright (c) 2022 bitlap.org */
package org.bitlap.client

import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ RowSet, TableSchema }

/** This class mainly wraps synchronous calls.
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0
 */
class BitlapClient(uri: String, port: Int, props: Map[String, String]) {

  private lazy val rpcClient: BitlapSyncClient = new BitlapSyncClient(uri, port, props)

  def openSession(): SessionHandle =
    rpcClient
      .openSession(username = "", password = "", configuration = props)
  // TODO: Add heartbeat

  def closeSession(sessionHandle: SessionHandle): Unit =
    rpcClient.closeSession(sessionHandle)

  def executeStatement(sessionHandle: SessionHandle, statement: String, queryTimeout: Long): OperationHandle =
    rpcClient
      .executeStatement(
        statement = statement,
        sessionHandle = sessionHandle,
        queryTimeout = queryTimeout,
        confOverlay = props
      )

  def fetchResults(operationHandle: OperationHandle, maxRows: Int, fetchType: Int): RowSet =
    rpcClient.fetchResults(operationHandle, maxRows, fetchType).results

  // make it return schema data, instead of `BOperationHandle` ?
  def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): OperationHandle = rpcClient.getSchemas(sessionHandle, catalogName, schemaName)

  def getTables(
    sessionHandle: SessionHandle,
    tableName: String,
    schemaName: String
  ): OperationHandle = ???

  def getColumns(
    sessionHandle: SessionHandle,
    tableName: String,
    schemaName: String,
    columnName: String
  ): OperationHandle = rpcClient.getColumns(sessionHandle, schemaName, tableName, columnName)

  def getResultSetMetadata(operationHandle: OperationHandle): TableSchema =
    rpcClient.getResultSetMetadata(operationHandle)
}
