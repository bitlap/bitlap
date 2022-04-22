/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.backend

import org.bitlap.network.dsl.blocking
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ RowSet, TableSchema }
import org.bitlap.network.rpc.{ Identity, RpcF }

/**
 * Sync implementation for jdbc server.
 *
 * @author 梦境迷离
 * @since 2022/04/22
 * @version 1.0
 */
class SyncRpcBackend extends RpcF[Identity] {

  private lazy val delegateBackend = new ZioRpcBackend()

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): Identity[SessionHandle] = blocking {
    delegateBackend.openSession(username, password, configuration)
  }

  override def closeSession(sessionHandle: SessionHandle): Identity[Unit] = blocking {
    delegateBackend.closeSession(sessionHandle)
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String]
  ): Identity[OperationHandle] = blocking {
    delegateBackend.executeStatement(sessionHandle, statement, confOverlay)
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): Identity[OperationHandle] = blocking {
    delegateBackend.executeStatement(
      sessionHandle,
      statement,
      queryTimeout,
      confOverlay
    )
  }

  override def fetchResults(opHandle: OperationHandle): Identity[RowSet] = blocking {
    delegateBackend.fetchResults(opHandle)
  }

  override def getResultSetMetadata(opHandle: OperationHandle): Identity[TableSchema] = blocking {
    delegateBackend.getResultSetMetadata(opHandle)
  }

  override def getColumns(
    sessionHandle: SessionHandle,
    tableName: String = null,
    schemaName: String = null,
    columnName: String = null
  ): Identity[OperationHandle] = blocking {
    delegateBackend.getColumns(sessionHandle, tableName, schemaName, columnName)
  }

  override def getDatabases(pattern: String): Identity[List[String]] = blocking {
    delegateBackend.getDatabases(pattern)
  }

  override def getTables(database: String, pattern: String): Identity[List[String]] = blocking {
    delegateBackend.getTables(database, pattern)
  }
}
