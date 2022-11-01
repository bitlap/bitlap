/* Copyright (c) 2022 bitlap.org */
package org.bitlap.client

import org.bitlap.network.handles._
import org.bitlap.network.models._

/** 供JDBC使用的同步客户端，本身无逻辑，全部都委托给异步客户端。但可以为其添加JDBC专属逻辑。
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0
 */
class BitlapClient(serverPeers: Array[String], props: Map[String, String]) {

  private lazy val rpcClient: BitlapSyncClient = new BitlapSyncClient(serverPeers, props)

  def openSession(): SessionHandle =
    rpcClient
      .openSession(username = "", password = "", configuration = props)

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
