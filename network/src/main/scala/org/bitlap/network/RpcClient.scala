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

  def syncOpenSession(request: BOpenSessionReq): BOpenSessionResp = {
    val ret = runtime.unsafeRun(openSession(request))
    verifySuccess(ret.getStatus)
    ret
  }

  def syncCloseSession(request: BCloseSessionReq): BCloseSessionResp = {
    val ret = runtime.unsafeRun(closeSession(request))
    verifySuccess(ret.getStatus)
    ret
  }

  def syncExecuteStatement(request: BExecuteStatementReq): BExecuteStatementResp = {
    val ret = runtime.unsafeRun(executeStatement(request))
    verifySuccess(ret.getStatus)
    ret
  }

  /**
   * Used to verify whether the RPC result is correct.
   */
  @inline private def verifySuccess(status: BStatus): Unit =
    if (status.statusCode != BStatusCode.B_STATUS_CODE_SUCCESS_STATUS) {
      throw new Exception(status.errorMessage)
    }

}
