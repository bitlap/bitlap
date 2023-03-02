/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.rpc

import org.bitlap.server.ServerProvider
import scalapb.zio_grpc._
import zio._
import zio.console._
import org.bitlap.network.ServerType
import org.bitlap.server.BitlapServerContext

/** bitlap grpc服务
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
final class GrpcServerProvider(override val port: Int) extends ServerProvider with ServerMain {

  private final lazy val backend = AsyncRpcBackend()
  private final lazy val liver   = GrpcServiceLive(backend)

  override def welcome: ZIO[zio.ZEnv, Throwable, Unit] =
    putStrLn(s"$serverType: Server is listening to port: $port")

  def services: ServiceList[zio.ZEnv] =
    ServiceList.addM(ZIO.succeed(liver)) // 可以随意更换实现

  override def service(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (BitlapServerContext.fillRpc(backend) *> super.run(args) *> ZIO.never)
      .onInterrupt(putStrLn(s"$serverType: Server stopped").ignore)

  override def serverType: ServerType = ServerType.Grpc
}
