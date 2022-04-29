/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.backend

import org.bitlap.network.dsl.blocking
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ FetchResults, TableSchema }
import org.bitlap.network.rpc.{ Identity, RpcF }
import org.bitlap.tools.apply

/**
 * Sync implementation for jdbc server.
 *
 * @author 梦境迷离
 * @since 2022/04/22
 * @version 1.0
 */
@apply
class SyncRpcBackend(private val delegateBackend: ZioRpcBackend) extends RpcF[Identity] {

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

  override def fetchResults(opHandle: OperationHandle): Identity[FetchResults] = blocking {
    delegateBackend.fetchResults(opHandle)
  }

  override def getResultSetMetadata(opHandle: OperationHandle): Identity[TableSchema] = blocking {
    delegateBackend.getResultSetMetadata(opHandle)
  }

  override def getColumns(
    sessionHandle: SessionHandle,
    schemaName: String,
    tableName: String,
    columnName: String
  ): Identity[OperationHandle] = blocking {
    delegateBackend.getColumns(sessionHandle, schemaName, tableName, columnName)
  }

  override def getDatabases(pattern: String): Identity[OperationHandle] = blocking {
    delegateBackend.getDatabases(pattern)
  }

  override def getTables(database: String, pattern: String): Identity[OperationHandle] = blocking {
    delegateBackend.getTables(database, pattern)
  }

  override def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): Identity[OperationHandle] = blocking {
    delegateBackend.getSchemas(sessionHandle, catalogName, schemaName)
  }
}
