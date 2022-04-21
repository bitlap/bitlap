/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network.client

import io.grpc.ManagedChannelBuilder
import org.bitlap.network.driver.proto.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.driver.proto.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.driver.proto.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.network.driver.service.ZioService.DriverServiceClient
import scalapb.zio_grpc.ZManagedChannel
import zio.{ Layer, ZIO }

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
trait ClientEF[F[_, _, _]] {

  def openSession(request: BOpenSessionReq): F[Any, Throwable, BOpenSessionResp]

  def closeSession(request: BCloseSessionReq): F[Any, Throwable, BCloseSessionResp]

  def executeStatement(request: BExecuteStatementReq): F[Any, Throwable, BExecuteStatementResp]

}

object ClientEF {

  def newZIOClient(uri: String, port: Int) = new ZIOClient(uri, port)

  class ZIOClient(uri: String, port: Int) extends ClientEF[ZIO] {

    val clientLayer: Layer[Throwable, DriverServiceClient] = DriverServiceClient.live(
      ZManagedChannel(ManagedChannelBuilder.forAddress(uri, port))
    )

    override def openSession(request: BOpenSessionReq): ZIO[Any, Throwable, BOpenSessionResp] =
      DriverServiceClient
        .openSession(request)
        .mapError(st => new Throwable(exception(st)))
        .provideLayer(clientLayer)

    override def closeSession(request: BCloseSessionReq): ZIO[Any, Throwable, BCloseSessionResp] =
      DriverServiceClient
        .closeSession(request)
        .mapError(st => new Throwable(exception(st)))
        .provideLayer(clientLayer)

    override def executeStatement(request: BExecuteStatementReq): ZIO[Any, Throwable, BExecuteStatementResp] =
      DriverServiceClient
        .executeStatement(request)
        .mapError(st => new Throwable(exception(st)))
        .provideLayer(clientLayer)
  }

}
