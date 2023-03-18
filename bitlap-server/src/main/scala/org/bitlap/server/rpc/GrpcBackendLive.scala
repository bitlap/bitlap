/* Copyright (c) 2023 bitlap.org */
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
import org.bitlap.network.enumeration.{ GetInfoType, OperationState }
import zio.blocking.Blocking
import zio.magic.ZioProvideMagicOps

/** 异步RPC的服务端实现，基于 zio 1.0
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
object GrpcBackendLive {
  private[server] lazy val liveInstance: GrpcBackendLive = GrpcBackendLive.apply()
  lazy val live: ULayer[Has[DriverAsyncRpc]]             = ZLayer.succeed(liveInstance)
}
@apply
class GrpcBackendLive extends DriverAsyncRpc {

  zio.Runtime.global.unsafeRun(SessionManager.startListener().provideLayer(SessionManager.live))

  // 底层都基于ZIO，错误使用 IO.failed(new Exception)
  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, SessionHandle] =
    SessionManager
      .openSession(username, password, configuration)
      .map { session =>
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
      .inject(SessionManager.live, Blocking.live)

  override def closeSession(sessionHandle: SessionHandle): ZIO[Any, Throwable, Unit] =
    SessionManager.closeSession(sessionHandle).inject(SessionManager.live, Blocking.live)

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, OperationHandle] =
    SessionManager
      .getSession(sessionHandle)
      .mapBoth(
        f => new SQLExecutedException(s"Unsupported SQL: $statement", f.getCause),
        session =>
          session.executeStatement(
            statement,
            confOverlay,
            queryTimeout
          )
      )
      .inject(SessionManager.live, Blocking.live)

  override def fetchResults(
    opHandle: OperationHandle,
    maxRows: Int,
    fetchType: Int
  ): ZIO[Any, Throwable, FetchResults] =
    (for {
      operation <- SessionManager
        .getOperation(opHandle)
      rs <- operation.parentSession.fetchResults(operation.opHandle)
    } yield FetchResults(hasMoreRows = false, rs)).inject(SessionManager.live, Blocking.live)

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    (for {
      operation <- SessionManager.getOperation(opHandle)
      rs        <- operation.parentSession.getResultSetMetadata(opHandle)
    } yield rs).inject(SessionManager.live, Blocking.live)

  override def getDatabases(sessionHandle: SessionHandle, pattern: String): ZIO[Any, Throwable, OperationHandle] = ???

  override def getTables(
    sessionHandle: SessionHandle,
    database: String,
    pattern: String
  ): ZIO[Any, Throwable, OperationHandle] = ???

  override def cancelOperation(opHandle: OperationHandle): Task[Unit] =
    SessionManager
      .getOperation(opHandle)
      .map { operation =>
        val session = operation.parentSession
        session.cancelOperation(opHandle)
      }
      .inject(SessionManager.live, Blocking.live)

  override def closeOperation(opHandle: OperationHandle): Task[Unit] =
    SessionManager
      .getOperation(opHandle)
      .map { operation =>
        val session = operation.parentSession
        session.closeOperation(opHandle)
      }
      .inject(SessionManager.live, Blocking.live)

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    Task.succeed(OperationStatus(Some(true), Some(OperationState.FinishedState)))

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] =
    SessionManager
      .getInfo(sessionHandle, getInfoType)
      .inject(SessionManager.live, Blocking.live)

}
