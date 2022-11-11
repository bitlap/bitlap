/* Copyright (c) 2022 bitlap.org */
package org.bitlap.client

import org.bitlap.network.handles._
import org.bitlap.network.models._
import org.bitlap.network.OperationState

/** 供JDBC使用的同步客户端，本身无逻辑，全部都委托给异步客户端。但可以为其添加JDBC专属逻辑。
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0
 */
class BitlapClient(serverPeers: Array[String], props: Map[String, String]) {

  private lazy val rpcClient: SyncClient = new SyncClient(serverPeers, props)

  def openSession(
    username: String = "",
    password: String = "",
    config: Map[String, String] = Map.empty
  ): SessionHandle =
    rpcClient
      .openSession(username, password, config)

  def closeSession(sessionHandle: SessionHandle): Unit =
    rpcClient.closeSession(sessionHandle)

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    config: Map[String, String] = Map.empty
  ): OperationHandle =
    rpcClient
      .executeStatement(
        statement = statement,
        sessionHandle = sessionHandle,
        queryTimeout = queryTimeout,
        confOverlay = config
      )

  def fetchResults(operationHandle: OperationHandle, maxRows: Int, fetchType: Int): RowSet =
    rpcClient.fetchResults(operationHandle, maxRows, fetchType).results

  def getTables(
    sessionHandle: SessionHandle,
    database: String,
    pattern: String
  ): OperationHandle = rpcClient
    .getTables(sessionHandle, database, pattern)

  def getDatabases(
    sessionHandle: SessionHandle,
    pattern: String
  ): OperationHandle = rpcClient.getDatabases(sessionHandle, pattern)

  def getResultSetMetadata(operationHandle: OperationHandle): TableSchema =
    rpcClient.getResultSetMetadata(operationHandle)

  def cancelOperation(opHandle: OperationHandle): Unit =
    rpcClient.cancelOperation(opHandle)

  def getOperationStatus(opHandle: OperationHandle): OperationState =
    rpcClient.getOperationStatus(opHandle)
}
