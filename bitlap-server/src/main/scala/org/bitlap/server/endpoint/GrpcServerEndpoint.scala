/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.endpoint

import org.bitlap.server.BitlapServerContext
import org.bitlap.server.config.BitlapGrpcConfig
import org.bitlap.server.rpc.{ AsyncRpcBackend, GrpcServiceLive }
import scalapb.zio_grpc._
import zio._
import zio.console.{ putStrLn, Console }

import java.io.IOException

/** bitlap grpc服务
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
object GrpcServerEndpoint {
  lazy val live: ZLayer[Has[BitlapGrpcConfig], Nothing, Has[GrpcServerEndpoint]] =
    ZLayer.fromService((config: BitlapGrpcConfig) => new GrpcServerEndpoint(config))

  def service(args: List[String]): ZIO[Console with Has[GrpcServerEndpoint], IOException, Unit] =
    (for {
      backend <- ZIO.serviceWith[GrpcServerEndpoint](_.liveBackend)
      _       <- BitlapServerContext.fillRpc(backend)
      _       <- putStrLn(s"Grpc Server started")
      _       <- ZIO.never
    } yield ())
      .onExit(_ => putStrLn(s"Grpc Server stopped").ignore)
}
final class GrpcServerEndpoint(val config: BitlapGrpcConfig) extends ServerMain {

  override def port: Int = config.port

  private final lazy val backend = AsyncRpcBackend()
  private final lazy val live    = GrpcServiceLive(backend)

  def liveBackend: ZIO[Has[GrpcServerEndpoint], Nothing, AsyncRpcBackend] =
    ZIO.serviceWith[GrpcServerEndpoint](_ => ZIO.succeed(backend))

  override def welcome: ZIO[zio.ZEnv, Throwable, Unit] =
    putStrLn(s"Grpc Server is listening to port: $port")

  def services: ServiceList[zio.ZEnv] =
    ServiceList.addM(ZIO.succeed(live)) // 可以随意更换实现
}
