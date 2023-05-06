/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server

import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import org.bitlap.network.DriverAsyncRpc
import org.bitlap.server.rpc.*
import scalapb.zio_grpc
import scalapb.zio_grpc.*
import zio.*
import org.bitlap.server.config.BitlapServerConfiguration

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
  ): ZIO[DriverAsyncRpc with GrpcServiceLive with Scope with GrpcServerEndpoint, Throwable, Unit] =
    (for {
      _ <- Console.printLine(s"Grpc Server started")
      _ <- BitlapContext.fillRpc(GrpcBackendLive.liveInstance)
      _ <- ZIO.serviceWithZIO[GrpcServerEndpoint](_.runGrpc())
      _ <- ZIO.never
    } yield ())
      .onInterrupt(_ => Console.printLine(s"Grpc Server was interrupted").ignore)

end GrpcServerEndpoint

final class GrpcServerEndpoint(val config: BitlapServerConfiguration):

  private def builder =
    ServerBuilder.forPort(config.grpcConfig.port).addService(ProtoReflectionService.newInstance())

  def runGrpc(): ZIO[DriverAsyncRpc with GrpcServiceLive with Scope, Throwable, ZEnvironment[zio_grpc.Server]] =
    ServerLayer
      .fromServiceList(
        builder.asInstanceOf[ServerBuilder[?]],
        ServiceList.accessEnv[DriverAsyncRpc, GrpcServiceLive]
      )
      .build
