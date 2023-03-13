/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.endpoint

import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import org.bitlap.network.AsyncRpc
import org.bitlap.network.driver.service.ZioService.ZDriverService.genericBindable
import org.bitlap.server.BitlapContext
import org.bitlap.server.config.BitlapGrpcConfig
import org.bitlap.server.rpc._
import scalapb.zio_grpc._
import zio._
import zio.console._
import zio.magic._

/** bitlap grpc服务
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
object GrpcServerEndpoint {
  lazy val live: ZLayer[Has[BitlapGrpcConfig], Nothing, Has[GrpcServerEndpoint]] =
    ZLayer.fromService((config: BitlapGrpcConfig) => new GrpcServerEndpoint(config))

  def service(args: List[String]): ZIO[Has[GrpcServerEndpoint] with Console, Throwable, Unit] =
    (for {
      _ <- putStrLn(s"Grpc Server started")
      _ <- BitlapContext.fillRpc(GrpcBackendLive.liveInstance)
      _ <- ZIO.serviceWith[GrpcServerEndpoint](_.runGrpc())
    } yield ())
      .onInterrupt(_ => putStrLn(s"Grpc Server was interrupted").ignore)
}
final class GrpcServerEndpoint(val config: BitlapGrpcConfig) {

  private def builder =
    ServerBuilder.forPort(config.port).addService(ProtoReflectionService.newInstance())

  def runGrpc(): ZIO[Any, Throwable, Nothing] =
    ServerLayer
      .fromServiceList(
        builder.asInstanceOf[ServerBuilder[_]],
        ServiceList.accessEnv[Has[AsyncRpc], GrpcServiceLive]
      )
      .build
      .useForever
      .inject(GrpcBackendLive.live, GrpcServiceLive.live)

}
