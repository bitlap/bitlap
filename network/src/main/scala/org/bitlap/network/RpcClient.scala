/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import io.grpc.{ ManagedChannelBuilder, Status }
import org.bitlap.network.driver.proto.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.driver.proto.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.driver.proto.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.network.driver.proto.{ BStatus, BStatusCode }
import org.bitlap.network.driver.service.ZioService.DriverServiceClient
import org.bitlap.tools.apply
import scalapb.zio_grpc.ZManagedChannel
import zio.{ Layer, _ }

/**
 * rpc client to wrapper sofa client
 */
@apply
class RpcClient(uri: String, port: Int) {

  private lazy val runtime = zio.Runtime.global

  val clientLayer: Layer[Throwable, DriverServiceClient] = DriverServiceClient.live(
    ZManagedChannel.apply(ManagedChannelBuilder.forAddress(uri, port))
  )

  def openSession(request: BOpenSessionReq): IO[Status, BOpenSessionResp] =
    DriverServiceClient.openSession(request).provideLayer(clientLayer)

  def closeSession(request: BCloseSessionReq): ZIO[Any, Status, BCloseSessionResp] =
    DriverServiceClient.closeSession(request).provideLayer(clientLayer)

  def executeStatement(request: BExecuteStatementReq): ZIO[Any, Status, BExecuteStatementResp] =
    DriverServiceClient.executeStatement(request).provideLayer(clientLayer)

  def syncOpenSession(request: BOpenSessionReq): BOpenSessionResp =
    blocking {
      openSession(request)
    } { t =>
      verifySuccess(t.getStatus, t)
    }

  def syncCloseSession(request: BCloseSessionReq): BCloseSessionResp =
    blocking {
      closeSession(request)
    } { t =>
      verifySuccess(t.getStatus, t)
    }

  def syncExecuteStatement(request: BExecuteStatementReq): BExecuteStatementResp =
    blocking {
      executeStatement(request)
    } { t =>
      verifySuccess(t.getStatus, t)
    }

  /**
   * Used to verify whether the RPC result is correct.
   */
  @inline private def verifySuccess[T](status: BStatus, t: T): T =
    if (t == null || status.statusCode != BStatusCode.B_STATUS_CODE_SUCCESS_STATUS) {
      throw new Exception(status.errorMessage)
    } else {
      t
    }

  @inline private def blocking[T, Z <: zio.ZIO[Any, Status, T]](action: => Z)(t: T => T): T =
    t.apply(runtime.unsafeRun(action))

}
