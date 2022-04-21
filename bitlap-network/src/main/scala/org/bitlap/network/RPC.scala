/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import io.grpc.Status
import org.bitlap.network.driver.proto.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.driver.proto.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.driver.proto.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.network.driver.service.ZioService.ZDriverService
import org.bitlap.network.helper.JdbcBackend
import org.bitlap.network.types.handles.SessionHandle
import org.bitlap.network.types.sqlStatus
import zio.{ IO, ZIO }

import scala.concurrent.Future

/**
 * RPC utils for network
 */
object RPC {

  def newClient(uri: String, port: Int): RpcClient = RpcClient(uri, port)

  case class FutureDriverServiceLive(jdbcHelper: JdbcBackend[Future]) extends ZDriverService[Any, Any] with sqlStatus {
    def openSession(request: BOpenSessionReq): IO[Status, BOpenSessionResp] =
      try {
        val handle = jdbcHelper.openSession(request.username, request.password, request.configuration)
        IO.fromFuture(make => handle)
          .map(hd =>
            BOpenSessionResp(
              successOpt(),
              configuration = request.configuration,
              sessionHandle = Some(hd.toBSessionHandle())
            )
          )
          .mapError(_ => Status.INTERNAL)

      } catch {
        case e: Exception =>
          IO.fail(Status.INTERNAL.withCause(e))
      }

    override def closeSession(request: BCloseSessionReq): ZIO[Any, Status, BCloseSessionResp] =
      try {
        val handle = jdbcHelper.closeSession(new SessionHandle(request.getSessionHandle))
        IO.fromFuture(make => handle).map(hd => BCloseSessionResp(successOpt())).mapError(_ => Status.INTERNAL)
      } catch {
        case e: Exception =>
          IO.fail(Status.INTERNAL.withCause(e))
      }

    override def executeStatement(request: BExecuteStatementReq): ZIO[Any, Status, BExecuteStatementResp] =
      try {
        val handle = jdbcHelper.executeStatement(
          new SessionHandle(request.getSessionHandle),
          request.statement,
          request.confOverlay
        )
        IO.fromFuture(make => handle)
          .map(hd => BExecuteStatementResp(successOpt(), Some(hd.toBOperationHandle())))
          .mapError(_ => Status.INTERNAL)
      } catch {
        case e: Exception =>
          IO.fail(Status.INTERNAL.withCause(e))
      }
  }

  case class SyncDriverServiceLive(jdbcHelper: JdbcBackend[Identity]) extends ZDriverService[Any, Any] with sqlStatus {
    def openSession(request: BOpenSessionReq): IO[Status, BOpenSessionResp] =
      try {
        val handle = jdbcHelper.openSession(request.username, request.password, request.configuration)
        val resp = BOpenSessionResp(
          successOpt(),
          configuration = request.configuration,
          sessionHandle = Some(handle.toBSessionHandle())
        )
        IO.effectTotal(resp)
      } catch {
        case e: Exception =>
          IO.fail(Status.INTERNAL.withCause(e))
      }

    override def closeSession(request: BCloseSessionReq): ZIO[Any, Status, BCloseSessionResp] =
      try {
        jdbcHelper.closeSession(new SessionHandle(request.getSessionHandle))
        val resp = BCloseSessionResp(successOpt())
        IO.effectTotal(resp)
      } catch {
        case e: Exception =>
          IO.fail(Status.INTERNAL.withCause(e))
      }

    override def executeStatement(request: BExecuteStatementReq): ZIO[Any, Status, BExecuteStatementResp] =
      try {
        val handle = jdbcHelper.executeStatement(
          new SessionHandle(request.getSessionHandle),
          request.statement,
          request.confOverlay
        )
        val resp = BExecuteStatementResp(successOpt(), Some(handle.toBOperationHandle()))
        IO.effectTotal(resp)
      } catch {
        case e: Exception =>
          IO.fail(Status.INTERNAL.withCause(e))
      }
  }
}
