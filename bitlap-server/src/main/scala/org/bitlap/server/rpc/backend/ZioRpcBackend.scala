/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.backend

import org.bitlap.network.OperationType
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ FetchResults, TableSchema }
import org.bitlap.network.rpc.RpcN
import org.bitlap.server.rpc.SessionManager
import zio.ZIO

/**
 * Async implementation based ZIO for jdbc server.
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
class ZioRpcBackend extends RpcN[ZIO] {

  private val sessionManager = new SessionManager()
  sessionManager.startListener()

  // 底层都基于ZIO，错误使用 IO.failed(new Exception)
  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, SessionHandle] =
    ZIO.effect {
      val session = sessionManager.openSession(username, password, configuration)
      session.sessionHandle
    }

  override def closeSession(sessionHandle: SessionHandle): ZIO[Any, Throwable, Unit] =
    ZIO.effect {
      sessionManager.closeSession(sessionHandle)
    }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, OperationHandle] = ZIO.effect {
    val session = sessionManager.getSession(sessionHandle)
    sessionManager.refreshSession(sessionHandle, session)
    session.executeStatement(
      sessionHandle,
      statement,
      confOverlay,
      queryTimeout
    )
  }

  override def fetchResults(opHandle: OperationHandle): ZIO[Any, Throwable, FetchResults] = ZIO.effect {
    val operation = sessionManager.operationManager.getOperation(opHandle)
    val session = operation.parentSession
    sessionManager.refreshSession(session.sessionHandle, session)
    FetchResults(false, session.fetchResults(opHandle))
  }

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] = ZIO.effect {
    val operation = sessionManager.operationManager.getOperation(opHandle)
    val session = operation.parentSession
    sessionManager.refreshSession(session.sessionHandle, session)
    session.getResultSetMetadata(opHandle)
  }

  override def getColumns(
    sessionHandle: SessionHandle,
    schemaName: String,
    tableName: String,
    columnName: String
  ): ZIO[Any, Throwable, OperationHandle] = ZIO.effect(new OperationHandle(OperationType.GET_COLUMNS))

  override def getDatabases(pattern: String): ZIO[Any, Throwable, OperationHandle] = ???

  override def getTables(database: String, pattern: String): ZIO[Any, Throwable, OperationHandle] = ???

  override def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): ZIO[Any, Throwable, OperationHandle] = ???
}
