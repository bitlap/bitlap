/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import io.grpc.{ManagedChannelBuilder, Status}
import org.bitlap.network.driver.proto.BCloseSession.{BCloseSessionReq, BCloseSessionResp}
import org.bitlap.network.driver.proto.BExecuteStatement.{BExecuteStatementReq, BExecuteStatementResp}
import org.bitlap.network.driver.proto.BOpenSession.{BOpenSessionReq, BOpenSessionResp}
import org.bitlap.network.driver.proto.{BStatus, BStatusCode}
import org.bitlap.network.driver.service.ZioService.DriverServiceClient
import org.bitlap.tools.apply
import scalapb.zio_grpc.ZManagedChannel
import zio.{Layer, _}

import scala.reflect.{ClassTag, classTag}

/**
 * rpc client to wrapper sofa client
 */
@apply
class RpcClient(uri: String, port: Int) {

  private lazy val runtime = zio.Runtime.global

  val clientLayer: Layer[Throwable, DriverServiceClient] = DriverServiceClient.live(
    ZManagedChannel(ManagedChannelBuilder.forAddress(uri, port))
  )
  
  def openSession(request: BOpenSessionReq): IO[Throwable, BOpenSessionResp] =
    DriverServiceClient.openSession(request)
      .mapError(st => new Throwable(exception(st)))
      .provideLayer(clientLayer)

  def closeSession(request: BCloseSessionReq): ZIO[Any, Throwable, BCloseSessionResp] = {
    DriverServiceClient.closeSession(request)
      .mapError(st => new Throwable(exception(st)))
      .provideLayer(clientLayer)
  }

  def executeStatement(request: BExecuteStatementReq): ZIO[Any, Throwable, BExecuteStatementResp] =
    DriverServiceClient.executeStatement(request)
      .mapError(st => new Throwable(exception(st)))
      .provideLayer(clientLayer)

  def syncOpenSession(request: BOpenSessionReq): BOpenSessionResp =
    blocking {
      openSession(request)
    } { t: BOpenSessionResp =>
      verifySuccess(t.getStatus, t)
    }

  def syncCloseSession(request: BCloseSessionReq): BCloseSessionResp =
    blocking {
      closeSession(request)
    } { t: BCloseSessionResp =>
      verifySuccess(t.getStatus, t)
    }

  def syncExecuteStatement(request: BExecuteStatementReq): BExecuteStatementResp =
    blocking {
      executeStatement(request)
    } { t:BExecuteStatementResp =>
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

  @inline private def blocking[T:ClassTag,Z:ClassTag](action: => Z)(t: T => T): T = {
    classTag[T].runtimeClass match {
      case clazz if clazz.isAssignableFrom(classOf[ZIO[Any, Throwable, T]]) =>
        t.apply(runtime.unsafeRun(action.asInstanceOf[ZIO[Any, Throwable, T]]))
    }
  }

  @inline private def exception(st: Status) = NetworkException(st.getCode.value(),st.getCode.toStatus.getDescription)
}
