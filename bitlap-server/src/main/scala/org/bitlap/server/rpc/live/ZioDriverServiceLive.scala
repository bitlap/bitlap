/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.live

import io.grpc.Status
import org.bitlap.network.RpcStatus
import org.bitlap.network.rpc.RpcN
import org.bitlap.network.driver.proto.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.driver.proto.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.driver.proto.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.network.driver.service.ZioService.ZDriverService
import org.bitlap.network.handles.SessionHandle
import org.bitlap.server.rpc.backend.ZioRpcBackend
import zio.ZIO

/**
 * A zio-grpc server implement by zio backend.
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
case class ZioDriverServiceLive() extends ZDriverService[Any, Any] with RpcStatus {

  private lazy val zioRpcBackend: RpcN[ZIO] = new ZioRpcBackend()

  def openSession(request: BOpenSessionReq): ZIO[Any, Status, BOpenSessionResp] =
    zioRpcBackend
      .openSession(request.username, request.password, request.configuration)
      .map(shd =>
        BOpenSessionResp(
          successOpt(),
          configuration = request.configuration,
          sessionHandle = Some(shd.toBSessionHandle())
        )
      )
      .mapError(_ => Status.INTERNAL) // TODO 不同异常返回不同Status

  override def closeSession(request: BCloseSessionReq): ZIO[Any, Status, BCloseSessionResp] =
    zioRpcBackend
      .closeSession(new SessionHandle(request.getSessionHandle))
      .map(_ => BCloseSessionResp(successOpt()))
      .mapError(_ => Status.INTERNAL)

  override def executeStatement(request: BExecuteStatementReq): ZIO[Any, Status, BExecuteStatementResp] =
    zioRpcBackend
      .executeStatement(
        new SessionHandle(request.getSessionHandle),
        request.statement,
        request.confOverlay
      )
      .map(hd => BExecuteStatementResp(successOpt(), Some(hd.toBOperationHandle())))
      .mapError(_ => Status.INTERNAL)
}
