/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server

import org.bitlap.network.Driver.ZioDriver.*
import org.bitlap.network.DriverIO
import org.bitlap.server.config.BitlapServerConfiguration
import org.bitlap.server.service.*

import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import scalapb.zio_grpc
import scalapb.zio_grpc.*
import zio.*

/** bitlap grpc服务
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
object GrpcServerEndpoint:

  lazy val live: ZLayer[BitlapServerConfiguration, Nothing, GrpcServerEndpoint] =
    ZLayer.fromFunction((config: BitlapServerConfiguration) => new GrpcServerEndpoint(config))

  def service(
    args: List[String]
  ): ZIO[DriverGrpcService with Scope with GrpcServerEndpoint, Throwable, Unit] =
    (for {
      _ <- Console.printLine(s"Grpc Server started")
      _ <- BitlapContext.fillRpc(DriverServiceLive.liveInstance)
      _ <- ZIO.serviceWithZIO[GrpcServerEndpoint](_.runGrpcServer())
      _ <- ZIO.never
    } yield ())
      .onInterrupt(_ => Console.printLine(s"Grpc Server was interrupted").ignore)

end GrpcServerEndpoint

final class GrpcServerEndpoint(val config: BitlapServerConfiguration):

  private val serverLayer =
    ServerLayer.fromServiceList(
      io.grpc.ServerBuilder.forPort(config.grpcConfig.port).addService(ProtoReflectionService.newInstance()),
      ServiceList
        .addFromEnvironment[ZDriverService[RequestContext]]
    )

  def runGrpcServer(): URIO[Any, ExitCode] = ZLayer
    .make[Server](
      serverLayer,
      DriverGrpcService.live,
      DriverServiceLive.live
    )
    .launch
    .exitCode
