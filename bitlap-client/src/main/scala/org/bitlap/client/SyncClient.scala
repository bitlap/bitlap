/* Copyright (c) 2022 bitlap.org */
package org.bitlap.client

import org.bitlap.network._
import org.bitlap.network.handles._
import org.bitlap.network.models._

/** 同步的RPC客户端，本身无逻辑，全部都委托给异步客户端。
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0
 */
class SyncClient(serverPeers: Array[String], props: Map[String, String]) extends SyncRpc with RpcStatus {

  private lazy val delegateClient = new AsyncClient(serverPeers, props)

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Identity[SessionHandle] = delegateClient.sync {
    _.openSession(username, password, configuration)
  }

  override def closeSession(sessionHandle: SessionHandle): Identity[Unit] = delegateClient.sync {
    _.closeSession(sessionHandle)
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): Identity[OperationHandle] = delegateClient.sync {
    _.executeStatement(sessionHandle, statement, queryTimeout, confOverlay)
  }

  override def fetchResults(opHandle: OperationHandle, maxRows: Int, fetchType: Int): Identity[FetchResults] =
    delegateClient.sync {
      _.fetchResults(opHandle, maxRows, fetchType)
    }

  override def getResultSetMetadata(opHandle: OperationHandle): Identity[TableSchema] = delegateClient.sync {
    _.getResultSetMetadata(opHandle)
  }

  override def getColumns(
    sessionHandle: SessionHandle,
    schemaName: String,
    tableName: String,
    columnName: String
  ): Identity[OperationHandle] = delegateClient.sync {
    _.getColumns(sessionHandle, tableName, schemaName, columnName)
  }

  override def getDatabases(pattern: String): Identity[OperationHandle] = delegateClient.sync {
    _.getDatabases(pattern)
  }
  override def getTables(database: String, pattern: String): Identity[OperationHandle] = delegateClient.sync {
    _.getTables(database, pattern)
  }

  override def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): Identity[OperationHandle] = delegateClient.sync {
    _.getSchemas(sessionHandle, catalogName, schemaName)
  }

  def getLeader(requestId: String): Identity[ServerAddress] = delegateClient.sync {
    _.getLeader(requestId)
  }
}
