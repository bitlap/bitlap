/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc

import org.bitlap.server.ServerProvider
import scalapb.zio_grpc.{ ServerMain, ServiceList }
import zio._
import zio.console._

/** @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
final class InternalGrpcServerProvider(override val port: Int) extends ServerProvider with ServerMain {

  def services: ServiceList[zio.ZEnv] = ServiceList.addM(ZIO.succeed(ZioDriverServiceLive(ZioRpcBackend()))) // 可以随意更换实现

  override def service(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      _ <- putStrLn(s"$serverType: Server is listening to port: $port")
      _ <- super.run(args)
    } yield ()).foldM(
      e => ZIO.fail(e).exitCode,
      _ => ZIO.effectTotal(ExitCode.success)
    )

  override def serverType: String = "INTERNAL_GRPC"
}
