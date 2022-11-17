/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc

import org.bitlap.core._
import org.bitlap.jdbc.Constants
import org.bitlap.network._
import org.bitlap.network.handles._
import org.bitlap.network.models._
import org.bitlap.server.session.SessionManager
import org.bitlap.tools._
import zio._
import org.bitlap.common.exception.SQLExecutedException

/** 异步RPC的服务端实现，基于 zio 1.0
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
@apply
class AsyncRpcBackend extends AsyncRpc {

  private val sessionManager = SessionManager()
  sessionManager.startListener()

  // 底层都基于ZIO，错误使用 IO.failed(new Exception)
  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, SessionHandle] =
    ZIO.effect {
      val session     = sessionManager.openSession(username, password, configuration)
      val coreSession = BitlapContext.initSession(session.sessionHandle.handleId)
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
    ZIO.effect {
      val session = sessionManager.getSession(sessionHandle)
      session.executeStatement(
        statement,
        confOverlay,
        queryTimeout
      )
    }.mapError(f => new SQLExecutedException(s"Unsupported SQL: $statement", f.getCause))

  override def fetchResults(
    opHandle: OperationHandle,
    maxRows: Int,
    fetchType: Int
  ): ZIO[Any, Throwable, FetchResults] = ZIO.effect {
    val operation = sessionManager.getOperation(opHandle)
    val session   = operation.parentSession
    // 支持maxRows，指最多一次取多少数据，相当于分页。与jdbc的maxRows不同
    FetchResults(hasMoreRows = false, session.fetchResults(opHandle))
  }

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] = ZIO.effect {
    val operation = sessionManager.getOperation(opHandle)
    val session   = operation.parentSession
    session.getResultSetMetadata(opHandle)
  }

  override def getDatabases(sessionHandle: SessionHandle, pattern: String): ZIO[Any, Throwable, OperationHandle] = ???

  override def getTables(
    sessionHandle: SessionHandle,
    database: String,
    pattern: String
  ): ZIO[Any, Throwable, OperationHandle] = ???

  override def cancelOperation(opHandle: OperationHandle): Task[Unit] = {
    val operation = sessionManager.getOperation(opHandle)
    val session   = operation.parentSession
    Task.effect(session.cancelOperation(opHandle))
  }

  override def closeOperation(opHandle: OperationHandle): Task[Unit] = {
    val operation = sessionManager.getOperation(opHandle)
    val session   = operation.parentSession
    Task.effect(session.closeOperation(opHandle))
  }

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    Task.succeed(OperationStatus(Some(true), Some(OperationState.FinishedState)))

}
