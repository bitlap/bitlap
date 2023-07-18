/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.rpc

import org.bitlap.common.exception.BitlapException
import org.bitlap.jdbc.Constants
import org.bitlap.network.*
import org.bitlap.network.NetworkException.SQLExecutedException
import org.bitlap.network.enumeration.*
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import org.bitlap.server.session.SessionManager

import com.typesafe.scalalogging.LazyLogging

import zio.*

/** 异步RPC的服务端实现，基于 zio 2.0
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
object GrpcBackendLive:
  private[server] lazy val liveInstance: GrpcBackendLive = new GrpcBackendLive
  lazy val live: ULayer[DriverIO]                        = ZLayer.succeed(liveInstance)
end GrpcBackendLive

final class GrpcBackendLive extends DriverIO with LazyLogging:

  // 底层都基于ZIO，错误使用 IO.failed(new Exception)
  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, SessionHandle] =
    SessionManager
      .openSession(username, password, configuration)
      .map { session =>
        session.currentSchema = configuration.getOrElse(Constants.DBNAME_PROPERTY_KEY, Constants.DEFAULT_DB)
        session.sessionHandle
      }
      .provide(SessionManager.live)

  override def closeSession(sessionHandle: SessionHandle): ZIO[Any, Throwable, Unit] =
    SessionManager.closeSession(sessionHandle).provide(SessionManager.live)

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, OperationHandle] =
    SessionManager
      .getSession(sessionHandle)
      .mapAttempt { session =>
        try
          session.executeStatement(
            statement,
            confOverlay,
            queryTimeout
          )
        catch {
          case bitlapException: BitlapException =>
            logger.error(s"Invalid SQL syntax: $statement", bitlapException)
            throw SQLExecutedException(
              s"Invalid SQL syntax: ${bitlapException.getCause.getLocalizedMessage}",
              Option(bitlapException)
            )
          case e: Throwable =>
            logger.error(s"Invalid SQL: $statement", e)
            throw SQLExecutedException(s"Invalid SQL: ${e.getLocalizedMessage}", Option(e))
        }
      }
      .provide(SessionManager.live)

  override def fetchResults(
    opHandle: OperationHandle,
    maxRows: Int,
    fetchType: Int
  ): ZIO[Any, Throwable, FetchResults] =
    (for {
      operation <- SessionManager
        .getOperation(opHandle)
      rs <- operation.parentSession.fetchResults(operation.opHandle)
    } yield FetchResults(hasMoreRows = false, rs)).provide(SessionManager.live)

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    (for {
      operation <- SessionManager.getOperation(opHandle)
      rs        <- operation.parentSession.getResultSetMetadata(opHandle)
    } yield rs).provide(SessionManager.live)

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
      .provide(SessionManager.live)

  override def closeOperation(opHandle: OperationHandle): Task[Unit] =
    SessionManager
      .getOperation(opHandle)
      .map { operation =>
        val session = operation.parentSession
        session.closeOperation(opHandle)
      }
      .provide(SessionManager.live)

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    ZIO.succeed(OperationStatus(Some(true), Some(OperationState.FinishedState)))

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] =
    SessionManager
      .getInfo(sessionHandle, getInfoType)
      .provide(SessionManager.live)
