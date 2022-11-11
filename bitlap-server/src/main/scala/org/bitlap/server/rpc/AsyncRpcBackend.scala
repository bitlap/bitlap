/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc

import org.bitlap.network.handles._
import org.bitlap.network.models._
import org.bitlap.network._
import org.bitlap.tools._
import zio._
import org.bitlap.server.session.SessionManager
import org.bitlap.core.utils.SqlParserUtil
import org.bitlap.network.NetworkException.SQLExecuteException
import org.bitlap.core.BitlapContext
import org.bitlap.core.SessionId
import org.bitlap.jdbc.Constants

/** 异步RPC的服务端实现，基于 zio 1.0
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
@apply
class AsyncRpcBackend extends AsyncRpc {

  private val sessionManager = new SessionManager()
  sessionManager.startListener()

  // 底层都基于ZIO，错误使用 IO.failed(new Exception)
  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, SessionHandle] =
    ZIO.effect {
      val session     = sessionManager.openSession(username, password, configuration)
      val coreSession = BitlapContext.getSession
      val newCoreSession = coreSession.copy(
        new SessionId(session.sessionHandle.handleId),
        session.sessionState,
        session.creationTime,
        configuration.getOrElse(Constants.DBNAME_PROPERTY_KEY, Constants.DEFAULT_DB)
      )
      BitlapContext.updateSession(newCoreSession)
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
  ): ZIO[Any, Throwable, OperationHandle] =
    ZIO.effect(SqlParserUtil.validateQuery(statement)).flatMap { q =>
      if (q) ZIO.effect {
        val session = sessionManager.getSession(sessionHandle)
        sessionManager.refreshSession(sessionHandle, session)
        session.executeStatement(
          sessionHandle,
          statement,
          confOverlay,
          queryTimeout
        )
      }
      else {
        ZIO.fail(SQLExecuteException(s"Unsupported SQL: $statement"))
      }

    }

  override def fetchResults(
    opHandle: OperationHandle,
    maxRows: Int,
    fetchType: Int
  ): ZIO[Any, Throwable, FetchResults] = ZIO.effect {
    val operation = sessionManager.operationManager.getOperation(opHandle)
    val session   = operation.parentSession
    sessionManager.refreshSession(session.sessionHandle, session)
    // 支持maxRows，指最多一次取多少数据，相当于分页。与jdbc的maxRows不同
    FetchResults(hasMoreRows = false, session.fetchResults(opHandle))
  }

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] = ZIO.effect {
    val operation = sessionManager.operationManager.getOperation(opHandle)
    val session   = operation.parentSession
    sessionManager.refreshSession(session.sessionHandle, session)
    session.getResultSetMetadata(opHandle)
  }

  override def getDatabases(sessionHandle: SessionHandle, pattern: String): ZIO[Any, Throwable, OperationHandle] = ???

  override def getTables(
    sessionHandle: SessionHandle,
    database: String,
    pattern: String
  ): ZIO[Any, Throwable, OperationHandle] = ???

  override def cancelOperation(opHandle: OperationHandle): Task[Unit] = {
    val operation = sessionManager.operationManager.getOperation(opHandle)
    val session   = operation.parentSession
    Task.effect(session.cancelOperation(opHandle))
  }

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationState] =
    Task.effect(OperationState.FinishedState)
}
