/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.live

import io.grpc.Status
import org.bitlap.network.RpcStatus
import org.bitlap.network.driver.proto.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.driver.proto.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.driver.proto.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.network.driver.service.ZioService.ZDriverService
import org.bitlap.network.dsl.zioFromFuture
import org.bitlap.network.rpc.RpcF
import org.bitlap.network.handles.SessionHandle
import org.bitlap.server.rpc.backend.FutureRpcBackend
import zio.ZIO

import scala.concurrent.Future

/**
 * A zio-grpc server implement by future backend.
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
case class FutureDriverServiceLive() extends ZDriverService[Any, Any] with RpcStatus {

  private lazy val futureRpcBackend: RpcF[Future] = new FutureRpcBackend

  def openSession(request: BOpenSessionReq): ZIO[Any, Status, BOpenSessionResp] = zioFromFuture {
    futureRpcBackend.openSession(request.username, request.password, request.configuration)
  } { hd =>
    BOpenSessionResp(
      successOpt(),
      configuration = request.configuration,
      sessionHandle = Some(hd.toBSessionHandle())
    )
  }

  override def closeSession(request: BCloseSessionReq): ZIO[Any, Status, BCloseSessionResp] = zioFromFuture {
    futureRpcBackend.closeSession(new SessionHandle(request.getSessionHandle))
  } { _ =>
    BCloseSessionResp(successOpt())
  }
  override def executeStatement(request: BExecuteStatementReq): ZIO[Any, Status, BExecuteStatementResp] =
    zioFromFuture {
      futureRpcBackend.executeStatement(
        new SessionHandle(request.getSessionHandle),
        request.statement,
        request.confOverlay
      )
    } { hd =>
      BExecuteStatementResp(successOpt(), Some(hd.toBOperationHandle()))
    }
}
