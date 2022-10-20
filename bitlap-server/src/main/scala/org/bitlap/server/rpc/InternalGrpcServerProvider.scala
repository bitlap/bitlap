/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc

import org.bitlap.server.rpc.backend.ZioRpcBackend
import org.bitlap.server.rpc.live.ZioDriverServiceLive
import org.bitlap.server.BitlapServerProvider
import scalapb.zio_grpc.{ ServerMain, ServiceList }
import zio._
import zio.console._

/** @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
final class InternalGrpcServerProvider(override val port: Int) extends BitlapServerProvider with ServerMain {

  def services: ServiceList[zio.ZEnv] = ServiceList.addM(ZIO.succeed(ZioDriverServiceLive(ZioRpcBackend()))) // 可以随意更换实现

  override def service(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      _ <- putStr(serverType + ": ")
      _ <- putStrLn("""
                      |    __    _ __  __          
                      |   / /_  (_) /_/ /___ _____ 
                      |  / __ \/ / __/ / __ `/ __ \
                      | / /_/ / / /_/ / /_/ / /_/ /
                      |/_.___/_/\__/_/\__,_/ .___/ 
                      |                   /_/   
                      |""".stripMargin)
    } yield ()).foldM(
      e => ZIO.fail(e).exitCode,
      _ => ZIO.effectTotal(ExitCode.success)
    )

  override def serverType: String = "INTERNAL_GRPC"
}