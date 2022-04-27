/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.backend

import org.bitlap.network.dsl.blocking
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models
import org.bitlap.network.models.FetchResults
import org.bitlap.network.rpc.RpcF
import org.bitlap.tools.apply

import scala.concurrent.Future

/**
 * Async implementation for jdbc server.
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
@apply
class FutureRpcBackend(private val delegateBackend: ZioRpcBackend) extends RpcF[Future] {

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Future[SessionHandle] = blocking {
    delegateBackend.openSession(username, password, configuration).toFuture
  }

  override def closeSession(sessionHandle: SessionHandle): Future[Unit] = blocking {
    delegateBackend.closeSession(sessionHandle).toFuture
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): Future[OperationHandle] = blocking {
    delegateBackend.executeStatement(sessionHandle, statement, queryTimeout, confOverlay).toFuture
  }

  override def fetchResults(opHandle: OperationHandle): Future[FetchResults] = blocking {
    delegateBackend.fetchResults(opHandle).toFuture
  }

  override def getResultSetMetadata(opHandle: OperationHandle): Future[models.TableSchema] = blocking {
    delegateBackend.getResultSetMetadata(opHandle).toFuture
  }

  override def getColumns(
    sessionHandle: SessionHandle,
    schemaName: String,
    tableName: String,
    columnName: String
  ): Future[OperationHandle] = blocking {
    delegateBackend.getColumns(sessionHandle, schemaName, tableName, columnName).toFuture
  }

  override def getDatabases(pattern: String): Future[OperationHandle] = blocking {
    delegateBackend.getDatabases(pattern).toFuture
  }

  override def getTables(database: String, pattern: String): Future[OperationHandle] = blocking {
    delegateBackend.getTables(database, pattern).toFuture
  }

  override def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): Future[OperationHandle] = blocking {
    delegateBackend.getSchemas(sessionHandle, catalogName, schemaName)
  }
}
